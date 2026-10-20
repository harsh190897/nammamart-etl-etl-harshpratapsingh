# NammaMart Quick Commerce: ETL & ELT Pipelines

**FDAIE Module 2, Data Engineering Foundations: assignment submission by Harsh Pratap Singh**

NammaMart is a fictional 10-minute grocery delivery company in Bengaluru. Its morning operations report is built by hand in Excel, arrives late, doesn't reconcile with the payment gateway, and passes customer personal data around in spreadsheets.

This project replaces that report with two Apache Airflow pipelines that run on the same data every day at **09:00 IST, Monday to Saturday**:

- **ETL** (`nammamart_etl_harshpratapsingh`): Extract → Validate → Transform → Load. Only rows that pass every critical quality check reach the finance tables; failed rows are quarantined with a reason.
- **ELT** (`nammamart_elt_harshpratapsingh`): Extract → Load raw → Transform in the warehouse with SQL (raw, staging and mart layers).

Both write to a local DuckDB warehouse and record every quality check (`dq_results`) and every run (`pipeline_runs`).

| Document | Contents |
|---|---|
| [SUBMISSION.md](SUBMISSION.md) | **The submission:** checklist, Data Issues Log, scheduling answers, data quality, business questions, governance, observability results, recommendation, screenshots |
| [assignment/ASSIGNMENT.md](assignment/ASSIGNMENT.md) | The assignment brief |
| [assignment/nammamart_dataset/README.md](assignment/nammamart_dataset/README.md) | The source dataset |

## Status

| Part | Status |
|---|---|
| 0. Data profiling and issues log | ✅ Done |
| A. ETL pipeline | ✅ Done (16 tasks: extract, validate, quarantine, transform, load, reconcile, report, run audit) |
| B. ELT pipeline | 🟡 Extract and raw layer done; staging and marts in progress |
| C. Scheduling | ✅ Answered (ETL) |
| D. Data quality dimensions | 🟡 ETL checks done (110 checks, all 6 dimensions); ELT checks in Part B |
| E. Data governance | ⬜ Not started |
| F. Observability tests | 🟡 Run audit (`pipeline_runs`) done for ETL; experiments not started |
| G. Recommendation | ⬜ Not started |

## Project structure

```
dags/
  nammamart_etl_harshpratapsingh.py   ETL DAG
  nammamart_elt_harshpratapsingh.py   ELT DAG
  meridian_pipeline.py                trainer's reference DAG (unchanged)
  exampledag.py                       Astro example DAG (unchanged)
include/
  nammamart_common.py                 shared paths, business rules, DuckDB connection, PII masking, audit tables
  nammamart_etl_checks.py             ETL data quality rules (pandas): passed / quarantined rows + dq_results
  nammamart_etl_transform.py          ETL transform (pandas): standardise, mask PII, join, derive measures
  nammamart_etl_load.py               ETL load, reconciliation and reports (DuckDB SQL)
  nammamart_elt.py                    ELT helpers: file snapshot + header check, run .sql files with run parameters
  data/nammamart/                     source CSV files (copied from assignment/nammamart_dataset, never edited)
  sql/raw/                            ELT raw layer: table definitions, load_raw, load_raw_increments
  quarantine/harshpratapsingh/        rejected rows with failure_reason (generated, not committed)
  tmp/harshpratapsingh/               per-run working files (generated, not committed)
  warehouse_harshpratapsingh.duckdb   DuckDB warehouse (generated, not committed)
profiling/                            Part 0 profiling scripts and the Data Issues Log CSVs
tests/                                unit tests (pytest) and Astro's DAG integrity test
docs/screenshots/                     screenshots referenced in SUBMISSION.md
```

## How to run

**Prerequisites:** Docker Desktop and the Astro CLI ([learner setup guide](learner_setup.docx)).

```bash
astro dev start                                          # start Airflow in Docker; UI at http://localhost:8080
astro dev run dags list-import-errors                    # check that the DAGs parse
astro dev run dags test nammamart_etl_harshpratapsingh   # run the ETL DAG once from the terminal
astro dev run dags test nammamart_elt_harshpratapsingh   # run the ELT DAG once from the terminal
astro dev pytest                                         # run all tests inside the Airflow container
astro dev stop
```

In the UI, unpause a DAG with its toggle to enable the 09:00 IST schedule, or press **Trigger** to run it now.

**Profiling and unit tests on your machine** (Python 3.9+):

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pandas duckdb pytest
python profiling/profile_files.py > profiling/profile_output.txt
python profiling/find_issues.py
pytest tests/test_nammamart_common.py tests/test_nammamart_etl.py tests/test_nammamart_elt.py -v
```

## Data and privacy

- `include/data/` holds the source files exactly as provided for the course, including customer personal data in `customers.csv`.
- Everything the pipelines generate (warehouse, quarantine files, per-run working files, `profiling/profile_output.txt`) can contain raw personal data and is excluded by [.gitignore](.gitignore).
- Reporting tables only contain masked email, phone and name.
- No credentials are stored in code; any secret (for example a Slack webhook) is read from an Airflow Variable.

---

## Appendix: Astro project reference

The original README generated by `astro dev init`.

### Overview

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

### Project Contents

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

### Deploy Your Project Locally

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

### Deploy Your Project to Astronomer

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

### Contact

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
