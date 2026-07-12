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
  status        VARCHAR(16)  NOT NULL,
  input         JSON         NULL,
  output        JSON         NULL,
  error_msg     VARCHAR(512) NULL,
  created_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uk_subtask_uuid (subtask_uuid),
  KEY idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
