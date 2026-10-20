-- Q6 (ETL): rows rejected in the latest successful ETL run, by file and by reason.
-- The ETL pipeline keeps rejected rows in quarantine CSV files, so this query reads those files
-- (getvariable('quarantine_glob') = include/quarantine/harshpratapsingh/*_failed_*.csv).
-- "(all reasons)" is the number of rejected rows; a row that broke two rules also counts once under each reason.
WITH latest AS (
    SELECT trim(regexp_replace(run_id, '[^A-Za-z0-9_-]+', '_', 'g'), '_') AS file_run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_etl_harshpratapsingh' AND status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1
),
rejected AS (
    SELECT q.source_file, q.failure_reason
    FROM read_csv(getvariable('quarantine_glob'), union_by_name = true, all_varchar = true, filename = true) AS q
    CROSS JOIN latest
    WHERE parse_filename(q.filename) = regexp_replace(q.source_file, '\.csv$', '') || '_failed_' || latest.file_run_id || '.csv'
),
by_reason AS (
    SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM rejected
)
SELECT * FROM (
    SELECT source_file, '(all reasons)' AS reason, count(*) AS rows_rejected FROM rejected GROUP BY ALL
    UNION ALL
    SELECT source_file, reason, count(*) FROM by_reason GROUP BY ALL
)
ORDER BY source_file, reason <> '(all reasons)', rows_rejected DESC, reason;
