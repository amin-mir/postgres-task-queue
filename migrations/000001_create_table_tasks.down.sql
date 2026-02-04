DROP INDEX IF EXISTS task_events_task_id_created_at_idx;
DROP TABLE IF EXISTS task_events;

DROP INDEX IF EXISTS tasks_dequeue_idx;
DROP INDEX IF EXISTS tasks_reaper_idx;
DROP TABLE IF EXISTS tasks;

DROP TYPE IF EXISTS task_status;