package com.agentflow.server.course;

import com.agentflow.server.coordinator.LectureIngestDecomposer;
import com.agentflow.server.entity.CourseEntity;
import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.CourseMapper;
import com.agentflow.server.mapper.LectureMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.service.TaskSubmitService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * 课程和讲次的写操作。加一讲 = 插一行讲次 + 提交一个 LECTURE_INGEST 任务。
 *
 * <p>这里的方法都不加 {@code @Transactional}:{@link TaskSubmitService#submit} 要等自己的事务提交后才发 Kafka,
 * 外面再包一层事务,消息会先于提交发出,worker 回传结果时 server 还查不到子任务。
 */
@Service
@RequiredArgsConstructor
public class LectureService {

    private static final int MAX_NAME = 200;
    private static final int MAX_TITLE = 300;
    private static final int MAX_SOURCE = 1024;
    private static final int MAX_ERROR = 512;

    private final CourseMapper courseMapper;
    private final LectureMapper lectureMapper;
    private final TaskMapper taskMapper;
    private final TaskSubmitService submitService;
    private final ObjectMapper objectMapper;

    public CourseEntity createCourse(String name) {
        String n = name == null ? "" : name.strip();
        if (n.isEmpty() || n.length() > MAX_NAME) {
            throw bad("课程名要 1 到 " + MAX_NAME + " 个字");
        }
        CourseEntity c = new CourseEntity();
        c.setName(n);
        courseMapper.insert(c);
        return c;
    }

    public LectureEntity addLecture(long courseId, String url, String path, String title) {
        if (courseMapper.selectById(courseId) == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "课程不存在");
        }
        String u = blankToNull(url);
        String p = blankToNull(path);
        if ((u == null) == (p == null)) {
            throw bad("链接和文件名要恰好填一个");
        }
        if ((u != null ? u : p).length() > MAX_SOURCE) {
            throw bad("链接或文件名太长");
        }
        String t = blankToNull(title);
        LectureEntity l = new LectureEntity();
        l.setCourseId(courseId);
        // ponytail: 序号取"现有最大值 + 1",同一门课同时加两讲会撞上唯一键,第二个请求报 500;个人使用不会并发加课
        List<LectureEntity> last = lectureMapper.selectList(new QueryWrapper<LectureEntity>()
                .eq("course_id", courseId).orderByDesc("seq").last("LIMIT 1"));
        l.setSeq(last.isEmpty() ? 1 : last.get(0).getSeq() + 1);
        l.setTitle(t == null ? null : cut(t, MAX_TITLE));
        l.setSourceUrl(u);
        l.setSourcePath(p);
        l.setStatus("INGESTING");
        lectureMapper.insert(l);
        submit(l);
        return l;
    }

    /**
     * 能重试的:失败的、部分完成的,以及"处理中"但背后已经没有在跑的任务的讲次
     * (进程死在插入讲次和提交任务之间,或者任务落定了但没能把状态写回来)——否则这样的讲次永远卡着。
     * 状态用一条带条件的 UPDATE 改:对失败和部分完成的讲次,连点两下只会提交一个任务。
     */
    public LectureEntity retry(long lectureId) {
        LectureEntity l = lectureMapper.selectById(lectureId);
        if (l == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "讲次不存在");
        }
        boolean orphaned = "INGESTING".equals(l.getStatus()) && !hasLiveTask(l.getTaskUuid());
        if (!orphaned && !"FAILED".equals(l.getStatus()) && !"PARTIAL".equals(l.getStatus())) {
            throw bad("只有失败、部分完成或处理中断的讲次可以重试");
        }
        // ponytail: 卡住的讲次状态本来就是 INGESTING,这条 UPDATE 拦不住同时点两下(会提交两个任务);
        // 两次跑的结果一样(媒体有缓存、索引整讲替换),先不为这个少见情况加锁
        int changed = lectureMapper.update(null, new UpdateWrapper<LectureEntity>()
                .eq("id", lectureId).eq("status", l.getStatus())
                .set("status", "INGESTING").set("error", null));
        if (changed == 0) {
            throw bad("这一讲的状态刚刚变了,请刷新后再试");
        }
        l.setStatus("INGESTING");
        l.setError(null);
        submit(l);
        return l;
    }

    /** 这个任务还会不会自己往下走(还没落定)。 */
    private boolean hasLiveTask(String taskUuid) {
        if (taskUuid == null) {
            return false;
        }
        TaskEntity task = taskMapper.selectOne(new QueryWrapper<TaskEntity>().eq("task_uuid", taskUuid));
        return task != null && ("PENDING".equals(task.getStatus()) || "RUNNING".equals(task.getStatus()));
    }

    private void submit(LectureEntity l) {
        try {
            ObjectNode payload = objectMapper.createObjectNode()
                    .put("lectureId", l.getId()).put("courseId", l.getCourseId());
            TaskEntity task = submitService.submit(LectureIngestDecomposer.TYPE, payload);
            l.setTaskUuid(task.getTaskUuid());
            // 只改这一列:任务可能已经跑完并把状态写回这一行了,整行覆盖会把结果冲掉
            lectureMapper.update(null, new UpdateWrapper<LectureEntity>()
                    .eq("id", l.getId()).set("task_uuid", task.getTaskUuid()));
        } catch (RuntimeException e) {
            lectureMapper.update(null, new UpdateWrapper<LectureEntity>().eq("id", l.getId())
                    .set("status", "FAILED").set("error", cut("提交摄入任务失败: " + e.getMessage(), MAX_ERROR)));
            throw e;
        }
    }

    private static ResponseStatusException bad(String reason) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, reason);
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.strip();
    }

    private static String cut(String s, int max) {
        return s.length() > max ? s.substring(0, max) : s;
    }
}
