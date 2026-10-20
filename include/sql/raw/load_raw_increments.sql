-- include/sql/raw/load_raw_increments.sql
-- Incremental load, raw step (ELT, Part B3): APPEND one next-day order file to raw_orders.
-- The file keeps its own name in _source_file (e.g. orders_2026-10-01_increment.csv), so staging can
-- apply it after orders.csv ("the later file wins"). Nothing in raw_orders is updated or deleted.
--
-- Variables: run_id, loaded_at, path_increment (snapshot of the increment file for this run)

INSERT INTO raw_orders BY NAME
SELECT coalesce(COLUMNS(* EXCLUDE (filename)), ''),
       parse_filename(filename)      AS _source_file,
       row_number() OVER () + 1      AS _source_row,
       getvariable('run_id')         AS _run_id,
       getvariable('loaded_at')      AS _loaded_at
FROM read_csv(getvariable('path_increment'), header = true, all_varchar = true, delim = ',', quote = '"', filename = true)
WHERE NOT EXISTS (SELECT 1 FROM raw_orders r
                  WHERE r._run_id = getvariable('run_id')
                    AND r._source_file = parse_filename(getvariable('path_increment')));
