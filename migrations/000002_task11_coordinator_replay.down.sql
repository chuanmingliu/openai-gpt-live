LOCK TABLE coordinator_work_journal IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM coordinator_work_journal WHERE delivered = FALSE) THEN
        RAISE EXCEPTION 'coordinator journal contains undelivered replay work' USING ERRCODE = '55000';
    END IF;
END;
$$;

DROP TABLE IF EXISTS coordinator_work_journal;
