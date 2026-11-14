package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.LectureMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

/**
 * LECTURE_INGEST:把一讲课录播变成可检索的片段。五步,每步一个普通的 Kafka 子任务:
 * 导入 → 转写 ∥ 截帧 → 建索引 → 知识点目录。
 *
 * <p>重活在媒体服务里做,子任务只带 mediaKey 和计数,逐字稿不经过 Kafka 和 MySQL。
 * 转写是一讲一个子任务、不切片:实测一张显卡上多个 worker 并行转写没有收益,提速来自 worker 内的批处理
 * (docs/benchmarks.md 第 7 节)。
 *
 * <p>失败的含义按"后面的步骤还能不能做"来定:导入或转写失败,索引建不出来,整讲失败;
 * 截帧或知识点目录失败,语音片段照样能检索,算部分完成。
 *
 * <p>payload:{@code {"lectureId":12,"courseId":3}}。链接和文件路径只从 lecture 表读:
 * 通用的任务提交接口对外开放,不能让调用方自带一个链接来触发下载。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LectureIngestDecomposer implements TaskDecomposer {

    public static final String TYPE = "LECTURE_INGEST";
    static final String IMPORT = "LEC_IMPORT";
    static final String TRANSCRIBE = "LEC_TRANSCRIBE";
    static final String FRAMES = "LEC_FRAMES";
    static final String INDEX = "LEC_INDEX";
    static final String CONCEPTS = "LEC_CONCEPTS";
    private static final int MAX_TITLE = 300;
    private static final int MAX_ERROR = 512;

    private final ObjectMapper objectMapper;
    private final LectureMapper lectureMapper;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        long lectureId = payload == null ? 0 : payload.path("lectureId").asLong(0);
        LectureEntity lecture = lectureId > 0 ? lectureMapper.selectById(lectureId) : null;
        if (lecture == null || !Objects.equals(lecture.getCourseId(), payload.path("courseId").asLong(0))) {
            throw new IllegalArgumentException("讲次不存在: lectureId=" + lectureId);
        }
        ObjectNode in = objectMapper.createObjectNode().put("lectureId", lectureId);
        if (lecture.getSourceUrl() != null) {
            in.put("url", lecture.getSourceUrl());
        } else if (lecture.getSourcePath() != null) {
            in.put("path", lecture.getSourcePath());
        } else {
            throw new IllegalArgumentException("讲次既没有链接也没有文件: lectureId=" + lectureId);
        }
        return List.of(new SubtaskDef(0, in.toString(), IMPORT));
    }

    @Override
    public List<SubtaskDef> nextStage(TaskEntity task, List<SubtaskEntity> subs) {
        SubtaskEntity imported = find(subs, IMPORT);
        if (!completed(imported)) {
            return List.of();
        }
        JsonNode payload = json(task.getPayload());
        JsonNode media = json(imported.getOutput());
        long lectureId = payload.path("lectureId").asLong();
        String mediaKey = media.path("mediaKey").asText();

        SubtaskEntity transcribed = find(subs, TRANSCRIBE);
        if (transcribed == null) {   // 转写占显卡、截帧占 CPU,互不依赖,并行
            String in = objectMapper.createObjectNode().put("lectureId", lectureId).put("mediaKey", mediaKey).toString();
            return List.of(new SubtaskDef(1, in, TRANSCRIBE), new SubtaskDef(2, in, FRAMES));
        }
        if (!completed(transcribed)) {
            return List.of();
        }
        SubtaskEntity indexed = find(subs, INDEX);
        if (indexed == null) {
            ObjectNode in = objectMapper.createObjectNode()
                    .put("courseId", payload.path("courseId").asLong())
                    .put("lectureId", lectureId)
                    .put("mediaKey", mediaKey)
                    .put("durationMs", media.path("durationMs").asLong())
                    .put("withSlides", completed(find(subs, FRAMES)));   // 截帧失败:只索引语音
            return List.of(new SubtaskDef(3, in.toString(), INDEX));
        }
        if (!completed(indexed) || find(subs, CONCEPTS) != null) {
            return List.of();
        }
        ObjectNode in = objectMapper.createObjectNode()
                .put("lectureId", lectureId)
                .put("mediaKey", mediaKey)
                .put("title", media.path("title").asText());
        if (payload.hasNonNull("model")) {
            in.put("model", payload.get("model").asText());
        }
        return List.of(new SubtaskDef(4, in.toString(), CONCEPTS));
    }

    /** 索引建成了才算有产物:没有索引一律 FAILED;有索引但截帧或知识点目录失败是 PARTIAL_FAILED。 */
    @Override
    public TaskStatus terminalStatus(TaskEntity task, List<SubtaskEntity> subs, TaskStatus computed) {
        if (!completed(find(subs, INDEX))) {
            return TaskStatus.FAILED;
        }
        boolean all = subs.stream().allMatch(LectureIngestDecomposer::completed) && find(subs, CONCEPTS) != null;
        return all ? TaskStatus.COMPLETED : TaskStatus.PARTIAL_FAILED;
    }

    /** 把结果写回讲次那一行:状态、媒体、知识点目录、失败原因。 */
    @Override
    public void onFinished(TaskEntity task, List<SubtaskEntity> subs) {
        long lectureId = json(task.getPayload()).path("lectureId").asLong();
        LectureEntity lecture = lectureMapper.selectById(lectureId);
        if (lecture == null) {
            log.warn("摄入任务落定时讲次已不存在 taskId={} lectureId={}", task.getId(), lectureId);
            return;
        }
        lecture.setStatus(switch (String.valueOf(task.getStatus())) {
            case "COMPLETED" -> "READY";
            case "PARTIAL_FAILED" -> "PARTIAL";
            default -> "FAILED";
        });
        SubtaskEntity imported = find(subs, IMPORT);
        if (completed(imported)) {
            JsonNode media = json(imported.getOutput());
            lecture.setMediaKey(media.path("mediaKey").asText());
            lecture.setDurationMs(media.path("durationMs").asLong());
            if (lecture.getTitle() == null || lecture.getTitle().isBlank()) {   // 用户填的标题优先
                lecture.setTitle(cut(media.path("title").asText(), MAX_TITLE));
            }
        }
        SubtaskEntity concepts = find(subs, CONCEPTS);
        lecture.setConcepts(completed(concepts) ? json(concepts.getOutput()).path("concepts").toString() : null);
        lecture.setError(subs.stream()
                .filter(s -> SubtaskStatus.FAILED.name().equals(s.getStatus()))
                .findFirst()
                .map(s -> cut(s.getKind() + ": " + s.getErrorMsg(), MAX_ERROR))
                .orElse(null));
        lecture.setUpdatedAt(null);   // 交给数据库的 ON UPDATE 刷新
        lectureMapper.updateById(lecture);
        log.info("讲次摄入落定 lectureId={} status={}", lectureId, lecture.getStatus());
    }

    private static SubtaskEntity find(List<SubtaskEntity> subs, String kind) {
        return subs.stream().filter(s -> kind.equals(s.getKind())).findFirst().orElse(null);
    }

    private static boolean completed(SubtaskEntity s) {
        return s != null && SubtaskStatus.COMPLETED.name().equals(s.getStatus());
    }

    private static String cut(String s, int max) {
        return s.length() > max ? s.substring(0, max) : s;
    }

    private JsonNode json(String s) {
        try {
            return objectMapper.readTree(s == null ? "{}" : s);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 解析失败:" + s, e);
        }
    }
}
