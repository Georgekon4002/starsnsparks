# Stars & Sparks ⭐🔥 — Advanced Database Topics 2026-27

Semester project for the NTUA course "Προχωρημένα Θέματα Βάσεων Δεδομένων" (Advanced Database Topics), academic year 2026–2027.

## Project overview

Big-data analysis of the **Yelp Open Dataset**, **GloVe 6B word embeddings**, and **ACS census data** using **Apache Spark 4.2.1** on a standalone cluster. Six analytical queries implemented with both PySpark DataFrame API and Spark SQL, with systematic performance measurement and execution-plan analysis.

## Team

| Name | Student ID |
|------|-----------|
| George | [ΑΜ] |
| Vicky | [ΑΜ] |

## Environment

| Component | Version / Spec |
|-----------|---------------|
| OS | Windows 11 + WSL2 (Ubuntu 24.04.5 LTS) |
| CPU | AMD Ryzen 5 7430U — 6 cores / 12 threads |
| RAM | 16 GB (WSL2: ~11 GiB) |
| Java | OpenJDK 17 |
| Python | 3.12.3 |
| Spark | 4.2.1 (spark-4.2.1-bin-hadoop3) |
| Cluster | Standalone: 1 master + 1 worker (8 cores, 8 GB) |

## Repository structure

```
starsnsparks/
├── src/                    # PySpark applications
│   ├── common.py           # SparkSession builder, schemas, timing helper
│   ├── etl_prepare.py      # Requirement 0: JSON/CSV/text → Parquet
│   ├── bench_formats.py    # Requirement 0: format comparison benchmark
│   ├── q1.py – q6.py       # Query implementations (DataFrame + SQL)
├── scripts/                # Bash scripts for spark-submit and cluster management
│   ├── cluster_start.sh    # Start master + worker + history server
│   ├── cluster_stop.sh     # Stop everything
│   ├── download_data.sh    # Download GloVe; instructions for Yelp & ACS
│   ├── run_etl.sh          # spark-submit for ETL
│   ├── run_bench_formats.sh
│   ├── run_q1.sh – run_q6.sh
│   └── benchmark.sh        # Run all benchmarks
├── conf/                   # Spark configuration templates
│   └── spark-defaults.conf.template
├── results/                # Timing results, explain() outputs (generated)
├── event_logs/             # Compressed Spark event logs (committed)
├── docs/                   # Project documentation
│   └── ROADMAP.md          # Detailed implementation plan
├── acs_zcta.csv            # ACS census data (small, kept in repo)
├── .gitignore
└── README.md
```

## Data preparation

The raw datasets are **not** included in this repository (except `acs_zcta.csv`). See [`docs/ROADMAP.md`](docs/ROADMAP.md) for download instructions.

1. **Yelp Open Dataset**: download from <https://www.yelp.com/dataset> (requires accepting terms)
2. **GloVe 6B 100d**: `wget https://nlp.stanford.edu/data/glove.6B.zip`
3. **ACS ZCTA**: included as `acs_zcta.csv`

Place all raw files in `~/advdb/raw/`. Run `bash scripts/run_etl.sh` to convert to Parquet in `~/advdb/parquet/`.

## Running experiments

```bash
# Start cluster
bash scripts/cluster_start.sh

# Run a specific query
bash scripts/run_q1.sh

# Run all benchmarks
bash scripts/benchmark.sh

# Stop cluster
bash scripts/cluster_stop.sh
```

Each script uses `spark-submit` with the appropriate executor configuration. Event logs are written to `~/spark-events/` and compressed copies are committed to `event_logs/`.

## Measurement protocol

- Wall-clock time around `collect()` or `write.format("noop")`
- ≥ 3 repetitions per configuration, each in a new Spark application
- Median and range reported
- No `cache()` between compared implementations (except where explicitly requested)
- Unique `appName` per experiment for History Server identification

## Submission

The final PDF report includes a link to this repository and the commit hash at submission time.
