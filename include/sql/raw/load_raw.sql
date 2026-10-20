-- include/sql/raw/load_raw.sql
-- Load today's five source files into the raw tables, exactly as received (ELT, Part B).
--
-- Variables set by the Airflow task (read with getvariable):
--   run_id, loaded_at                     this run
--   path_customers, path_products,
--   path_stores, path_orders, path_payments   snapshot of each source file for this run
--
-- How each INSERT works:
--   read_csv(..., all_varchar = true)    every column is read as text, nothing is converted
--   coalesce(COLUMNS(*), '')            an empty cell stays '' (DuckDB would otherwise make it NULL)
--   INSERT ... BY NAME                  columns are matched by name: a renamed or missing column fails loudly
--   row_number() OVER () + 1            CSV line number (line 1 is the header)
--   WHERE NOT EXISTS (...)              a file is loaded only once per run, so a retried task adds no duplicates
-- Every run appends its own batch: raw is never updated or deleted.

INSERT INTO raw_customers BY NAME
SELECT coalesce(COLUMNS(* EXCLUDE (filename)), ''),
       parse_filename(filename)      AS _source_file,
       row_number() OVER () + 1      AS _source_row,
       getvariable('run_id')         AS _run_id,
       getvariable('loaded_at')      AS _loaded_at
FROM read_csv(getvariable('path_customers'), header = true, all_varchar = true, delim = ',', quote = '"', filename = true)
WHERE NOT EXISTS (SELECT 1 FROM raw_customers r
                  WHERE r._run_id = getvariable('run_id')
                    AND r._source_file = parse_filename(getvariable('path_customers')));

INSERT INTO raw_products BY NAME
SELECT coalesce(COLUMNS(* EXCLUDE (filename)), ''),
       parse_filename(filename)      AS _source_file,
       row_number() OVER () + 1      AS _source_row,
       getvariable('run_id')         AS _run_id,
       getvariable('loaded_at')      AS _loaded_at
FROM read_csv(getvariable('path_products'), header = true, all_varchar = true, delim = ',', quote = '"', filename = true)
WHERE NOT EXISTS (SELECT 1 FROM raw_products r
                  WHERE r._run_id = getvariable('run_id')
                    AND r._source_file = parse_filename(getvariable('path_products')));

INSERT INTO raw_stores BY NAME
SELECT coalesce(COLUMNS(* EXCLUDE (filename)), ''),
       parse_filename(filename)      AS _source_file,
       row_number() OVER () + 1      AS _source_row,
       getvariable('run_id')         AS _run_id,
       getvariable('loaded_at')      AS _loaded_at
FROM read_csv(getvariable('path_stores'), header = true, all_varchar = true, delim = ',', quote = '"', filename = true)
WHERE NOT EXISTS (SELECT 1 FROM raw_stores r
                  WHERE r._run_id = getvariable('run_id')
                    AND r._source_file = parse_filename(getvariable('path_stores')));

INSERT INTO raw_orders BY NAME
SELECT coalesce(COLUMNS(* EXCLUDE (filename)), ''),
       parse_filename(filename)      AS _source_file,
       row_number() OVER () + 1      AS _source_row,
       getvariable('run_id')         AS _run_id,
       getvariable('loaded_at')      AS _loaded_at
FROM read_csv(getvariable('path_orders'), header = true, all_varchar = true, delim = ',', quote = '"', filename = true)
WHERE NOT EXISTS (SELECT 1 FROM raw_orders r
                  WHERE r._run_id = getvariable('run_id')
                    AND r._source_file = parse_filename(getvariable('path_orders')));

INSERT INTO raw_payments BY NAME
SELECT coalesce(COLUMNS(* EXCLUDE (filename)), ''),
       parse_filename(filename)      AS _source_file,
       row_number() OVER () + 1      AS _source_row,
       getvariable('run_id')         AS _run_id,
       getvariable('loaded_at')      AS _loaded_at
FROM read_csv(getvariable('path_payments'), header = true, all_varchar = true, delim = ',', quote = '"', filename = true)
WHERE NOT EXISTS (SELECT 1 FROM raw_payments r
                  WHERE r._run_id = getvariable('run_id')
                    AND r._source_file = parse_filename(getvariable('path_payments')));
