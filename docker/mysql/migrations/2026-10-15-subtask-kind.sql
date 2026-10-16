-- RESEARCH_AGENT:子任务可有自己的处理类型(PLAN/AGENT/SYNTHESIZE);旧任务为 NULL,沿用 task.type。
-- 新库由 init.sql 建好,无需执行;已有库执行一次即可。
ALTER TABLE subtask ADD COLUMN kind VARCHAR(16) NULL AFTER seq;
