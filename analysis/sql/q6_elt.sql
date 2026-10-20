-- Q6 (ELT): rows rejected in the latest successful ELT run, by file and by reason (from stg_rejected_rows).
-- "(all reasons)" is the number of rejected rows; a row that broke two rules also counts once under each reason.
WITH latest AS (
    SELECT run_id
    FROM pipeline_runs
    WHERE dag_id = 'nammamart_elt_harshpratapsingh' AND status = 'SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1
),
rejected AS (
    SELECT r.source_file, r.failure_reason
    FROM stg_rejected_rows AS r JOIN latest ON r._run_id = latest.run_id
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
