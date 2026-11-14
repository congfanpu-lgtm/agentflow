-- 课程录播(产品层)的 2 张表。新环境由 init.sql 建好;已有数据卷的环境手动执行一次:
--   docker exec -i agentflow-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" agentflow < docker/mysql/migrations/2026-11-13-course-video.sql

CREATE TABLE IF NOT EXISTS course (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(200) NOT NULL,
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lecture (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  course_id   BIGINT        NOT NULL,
  seq         INT           NOT NULL,          -- 课内序号,从 1 起,按添加顺序
  title       VARCHAR(300)  NULL,              -- 用户没填时,摄入完成后用视频自己的标题
  source_url  VARCHAR(1024) NULL,
  source_path VARCHAR(1024) NULL,
  status      VARCHAR(16)   NOT NULL,          -- INGESTING / READY / PARTIAL / FAILED
  media_key   CHAR(32)      NULL,
  duration_ms BIGINT        NULL,
  task_uuid   VARCHAR(36)   NULL,              -- 最近一次摄入任务
  concepts    JSON          NULL,              -- [{"name","startMs","summary"}]
  error       VARCHAR(512)  NULL,
  created_at  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_course_seq (course_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
