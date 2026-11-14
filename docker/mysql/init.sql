CREATE TABLE IF NOT EXISTS task (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  task_uuid     VARCHAR(36)  NOT NULL,
  type          VARCHAR(32)  NOT NULL,
  status        VARCHAR(16)  NOT NULL,
  payload       JSON         NULL,
  result        JSON         NULL,
  subtask_total INT          NOT NULL DEFAULT 0,
  subtask_done  INT          NOT NULL DEFAULT 0,
  subtask_failed INT         NOT NULL DEFAULT 0,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_task_uuid (task_uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS subtask (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  subtask_uuid  VARCHAR(36)  NOT NULL,
  task_id       BIGINT       NOT NULL,
  seq           INT          NOT NULL,
  kind          VARCHAR(16)  NULL,
  status        VARCHAR(16)  NOT NULL,
  input         JSON         NULL,
  output        JSON         NULL,
  error_msg     VARCHAR(512) NULL,
  redispatch_count INT NOT NULL DEFAULT 0,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_subtask_uuid (subtask_uuid),
  KEY idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ===== 产品层:股票每日简报(与 migrations/2026-11-09-stock-brief.sql 保持一致) =====
CREATE TABLE IF NOT EXISTS app_user (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  email         VARCHAR(254) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  webhook_url   VARCHAR(512) NULL,
  push_hour     TINYINT      NOT NULL DEFAULT 8,
  timezone      VARCHAR(64)  NOT NULL DEFAULT 'Asia/Shanghai',
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS watchlist (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id       BIGINT       NOT NULL,
  ticker        VARCHAR(10)  NOT NULL,
  company_name  VARCHAR(200) NOT NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_user_ticker (user_id, ticker),
  KEY idx_ticker (ticker)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 一行 = 某只股票的一件事。既是事件时间线,也是简报内容(某人的简报 = 其自选股的事件按 brief_date 分组)。
-- uk_ticker_url:同一只股票的同一个出处只记一次,任务重跑 / 两次定时运行都不会重复。
CREATE TABLE IF NOT EXISTS stock_event (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  ticker        VARCHAR(10)   NOT NULL,
  brief_date    DATE          NOT NULL,
  event_date    DATE          NULL,
  title         VARCHAR(200)  NOT NULL,
  summary       VARCHAR(1000) NOT NULL,
  importance    TINYINT       NOT NULL DEFAULT 1,
  url           VARCHAR(1024) NOT NULL,
  url_hash      CHAR(64)      NOT NULL,
  source_title  VARCHAR(300)  NULL,
  quote         VARCHAR(1000) NULL,
  form          VARCHAR(16)   NULL,
  created_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_ticker_url (ticker, url_hash),
  KEY idx_ticker_created (ticker, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 推送记录:唯一键保证"每人每件事只推一次",多实例同时投递也只有一个能插入成功。
CREATE TABLE IF NOT EXISTS push_log (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id       BIGINT      NOT NULL,
  event_id      BIGINT      NOT NULL,
  created_at    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_user_event (user_id, event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ===== 产品层:课程录播(与 migrations/2026-11-13-course-video.sql 保持一致) =====
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
