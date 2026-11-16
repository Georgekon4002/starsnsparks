# ROADMAP — Advanced Database Topics 2026-27

> Semester project: big-data analysis with Apache Spark 4.2.1 on Yelp, GloVe, and ACS datasets.

---

## Requirement 0 — Environment & Data Preparation (10%)

### What to implement

1. **Standalone cluster**: 1 master + 1 worker (SPARK_WORKER_CORES=8, SPARK_WORKER_MEMORY=8g), History Server, event logs enabled.
2. **Yelp → Parquet** with **explicit** `StructType` schemas:
   - `business.json` → `parquet/business/` — keep `attributes` and `hours` as STRING (later parsed with `parse_json` → VARIANT).
   - `review.json` → `parquet/review/` — **partitioned by year** (extract year from `date`).
   - `user.json` → `parquet/user/`.
3. **GloVe → Parquet** (`word STRING, vec ARRAY<FLOAT>`):
   - Read with `spark.read.text()`, split each line, first token = word, remaining 100 floats → `ARRAY<FLOAT>` via `slice` + `transform`.
4. **ACS → Parquet** (`zcta STRING, median_household_income INT, population INT`):
   - Read CSV with `;` delimiter, `zcta` as STRING (leading zeros!).
5. **Format benchmark** — query: "average review stars per year":
   - (a) JSON with schema inference
   - (b) JSON with explicit schema
   - (c) Parquet (partitioned by year)
   - Report `explain()` output for each (ReadSchema, PushedFilters, PartitionFilters).
   - Report disk sizes (JSON vs Parquet).

### Spark features / APIs

- `StructType` / `StructField` with explicit types
- `spark.read.json(path, schema=...)`, `spark.read.text()`, `spark.read.csv(path, sep=";", header=True, schema=...)`
- `df.write.parquet()`, `df.write.partitionBy("year").parquet()`
- `split()`, `slice()`, `transform()`, `col().cast("float")`
- `df.explain(mode="formatted")`

### Experiments & measurements

| Experiment | Config | Repetitions | Metric |
|---|---|---|---|
| Format benchmark (JSON-infer / JSON-explicit / Parquet) | 4 exec × (1c, 2GB) | ≥ 3 each, new app each | Wall-clock (median, range) |
| Disk size comparison | — | — | Bytes on disk |

### Report contents

- Hardware specs: CPU model, logical cores, RAM, disk type, OS.
- Cluster config: worker cores/memory, driver memory.
- Disk sizes table: JSON vs Parquet per dataset.
- Benchmark timings table + `explain()` excerpts.
- Discussion: why Parquet is faster (column pruning, predicate pushdown, partition pruning).

### ⚠️ Pitfalls

- **ANSI mode** (Tip 8): on by default in Spark 4.x. Use `try_cast` / `try_variant_get` for dirty data.
- **ZCTA as STRING** (Tip 6): reading as integer loses leading zeros (e.g., 08002 → 8002).
- **GloVe as text** (Tip 9): must read with `spark.read.text()`, not CSV — some "words" are punctuation or quotes.
- **ARRAY\<FLOAT\>** (Tip 9): vector functions require `ARRAY<FLOAT>`, not `ARRAY<DOUBLE>`.
- **attributes/hours as STRING** (Tip 7): declare as STRING in schema, convert to VARIANT with `parse_json()` at query time.

---

## Query 1 — Review Length Categories (15%)

### What to implement

Filter reviews: years 2015–2021, `useful >= 1`. Classify by text length (characters):
- Short: < 250
- Medium: 250 – 749
- Long: 750 – 1999
- Very Long: ≥ 2000

**(a)** Per category (descending by avg useful): count, avg stars, avg useful.
**(b)** Per year × category: percentage of that year's (filtered) reviews in each category.

Both DataFrame and SQL implementations.

### Spark features / APIs

- `length()` (character count, not bytes — Tip 2)
- `F.when().when()...otherwise()` for bucketing
- `groupBy()`, `agg()`, `avg()`, `count()`
- `Window` partitioning by year for percentages
- `cache()` / `unpersist()` for the cache experiment
- `df.explain(mode="formatted")` to inspect PartitionFilters, PushedFilters, ReadSchema

### Experiments & measurements

| Experiment | Config | What to observe |
|---|---|---|
| Identify PartitionFilters / PushedFilters / post-read Filter | 4×(1c,2GB) | `explain()` output |
| (a)+(b) sequential, **without** cache | 4×(1c,2GB) | Wall-clock |
| (a)+(b) sequential, **with** cache of filtered set | 4×(1c,2GB) | Wall-clock; Storage tab → cached size vs Parquet files |
| Cache **entire** review table (with text) | 4×(1c,2GB) | Expect OOM or disk spill — explain why |

### Report contents

- `explain()` annotated: which filters are partition, pushed, or post-read.
- ReadSchema — confirm `text` is read only when needed.
- Timing comparison: cache vs no-cache.
- Storage tab screenshot: cached size vs Parquet on disk.
- OOM explanation when caching full review table with only 8 GB total executor memory.

### ⚠️ Pitfalls

- Use `length()` not `octet_length()`.
- Year filter should hit PartitionFilters (reviews partitioned by year).
- `useful >= 1` → PushedFilter on Parquet.

---

## Query 2 — Top-3 Categories per State (15%)

### What to implement

For states with ≥ 1000 businesses, find the top-3 categories by avg review stars, considering only categories with ≥ 500 reviews in that state. Per category: count, avg_stars, % 5-star reviews. Order by state, then descending avg_stars; ties broken by higher review count.

Both DataFrame and SQL. SQL **must** use `row_number()` window function.

### Spark features / APIs

- `split(col("categories"), ", ")` + `explode()` to get one row per category
- `join` review ⋈ business on `business_id`
- `groupBy("state", "category")`, `agg(count, avg, sum(when(stars==5,1)))`
- SQL: `ROW_NUMBER() OVER (PARTITION BY state ORDER BY avg_stars DESC, cnt DESC)`
- DataFrame: `Window.partitionBy("state").orderBy(desc("avg_stars"), desc("cnt"))` + filter `rn <= 3`

### Experiments & measurements

| Experiment | Config | What to observe |
|---|---|---|
| Compare physical plans (DF vs SQL) | 4×(1c,2GB) | `explain(mode="formatted")` — are they identical? |
| Timing comparison | 4×(1c,2GB) | Median & range — does difference exceed inter-run variance? |
| Join strategy | — | What join does the optimizer pick for review ⋈ business? Why? |
| Shuffle count | — | How many Exchange nodes? What causes each? |

### Report contents

- Side-by-side physical plans.
- Discussion: join strategy (likely SortMerge for two large tables).
- Shuffle analysis: expect exchanges for join, groupBy, and window.
- Timing table.

### ⚠️ Pitfalls

- **Tip 1**: A review counts toward ALL categories of its business → explode creates many rows. The per-category counts don't sum to total review count.
- Filter states ≥ 1000 businesses BEFORE joining, or after — make sure the filter is on businesses, not reviews.

---

## Query 3 — Per Registration-Year User Analysis (15%)

### What to implement

Per `year(yelping_since)`:
- (a) total review count
- (b) avg review stars
- (c) avg "strictness": mean of `(review.stars - business.stars)`
- (d) % of reviews written in a year when the author was elite
- (e) count of users with zero reviews in review.json

Order by registration year ascending.

Both DataFrame and SQL.

### Spark features / APIs

- `year(col("yelping_since"))` for registration year
- Three-way join: review ⋈ user ⋈ business (for strictness)
- Elite check: parse the `elite` field (comma-separated years), check if `year(review.date)` is in the list
- Anti-join for (e): users NOT IN review.user_id

### Experiments & measurements

| Experiment | Config | What to observe |
|---|---|---|
| Default join strategies | 4×(1c,2GB) | `explain()` — what does Catalyst pick for review ⋈ user and the anti-join? |
| Hint: BROADCAST on review ⋈ user | 4×(1c,2GB) | Expect failure — user table is too large for broadcast (~3 GB) |
| Hint: MERGE (SortMergeJoin) | 4×(1c,2GB) | Timing |
| Hint: SHUFFLE_HASH | 4×(1c,2GB) | Timing |
| Hint: SHUFFLE_REPLICATE_NL | — | `explain()` only + estimate comparisons (6.99M × 1.99M ≈ 13.9 trillion). Optionally run on 0.1% sample. |
| AQE on vs off | 4×(1c,2GB) | Compare initial vs final plan in Spark UI |

### Report contents

- Default strategies and why.
- Timing table for each hint.
- If OOM occurs (e.g., BROADCAST), explain why.
- SHUFFLE_REPLICATE_NL: estimated comparison count.
- AQE comparison: plan before vs after.
- Conclusion: which strategy is best and why.

### ⚠️ Pitfalls

- **Tip 3 (elite anomalies)**: "20,20" should be treated as "2020". Need to clean: if a token is 2 digits (e.g., "20"), prepend "20" to make it 4 digits? Actually "20,20" likely means the string "2020" got split into "20" and "20" — need to investigate the actual data. Strategy: split on comma, trim, filter tokens that are 4-digit years. For 2-digit tokens: could be halves of a corrupted 4-digit year. **Inspect the data first.**
- Anti-join for (e): `user LEFT ANTI JOIN review ON user_id`.

---

## Query 4 — Nearest-Neighbor Restaurants in PA (15%)

### What to implement

Filter: `state='PA'`, `is_open=1`, `categories LIKE '%Restaurants%'`.

For each restaurant, find the 5 nearest others (haversine distance). Then per `postal_code`: count, avg NN distance (km), avg stars, avg NN stars. Order by count descending. Also compute Pearson correlation between each restaurant's stars and its avg NN stars.

Two implementations:
- **(a) Without NEAREST BY**: self-join (cross or inner), compute haversine for all pairs, rank by distance, take top 5 per restaurant.
- **(b) With NEAREST BY**: `FROM restaurants q JOIN restaurants t EXACT NEAREST 6 BY DISTANCE haversine(...)` then exclude self.

### Spark features / APIs

- Haversine: `radians()`, `sin()`, `cos()`, `asin()`, `sqrt()`, `pow()` — Tip 4
- `NEAREST BY` syntax — Tip 5
- `F.corr()` for Pearson correlation
- `Window.partitionBy("business_id").orderBy("distance")` for version (a)

### Experiments & measurements

| Experiment | Config | What to observe |
|---|---|---|
| Compare plans: (a) vs (b) | 4×(1c,2GB) | Join/aggregation operators |
| Intermediate pair count | — | Estimate: N² where N = # PA restaurants. Check shuffle write, spill in Spark UI. |
| Scaling test (version b) | 2 exec × 3GB, 1 core each | Timing |
| Scaling test (version b) | 2 exec × 3GB, 2 cores each | Timing |
| Scaling test (version b) | 2 exec × 3GB, 4 cores each | Timing |

### Report contents

- Plans for both versions.
- Pair count estimate vs actual (shuffle write, spill).
- Scaling table: cores vs time.
- Discussion: CPU-bound or I/O-bound? How does it scale?
- Pearson correlation result and interpretation.

### ⚠️ Pitfalls

- **Tip 5**: NEAREST BY with self-join → request k+1=6, then filter out `q.business_id == t.business_id`.
- Version (a) self-join produces N×(N−1) pairs — could be huge. Consider filtering `q.business_id < t.business_id` to halve pairs, then mirror.
- Haversine formula: d = 2R·asin(√(sin²(Δφ/2) + cos(φ₁)·cos(φ₂)·sin²(Δλ/2)))

---

## Query 5 — Restaurant Density & Income Correlation (20%)

### What to implement

Join businesses (open restaurants) with ACS on `postal_code = zcta`. Per postal_code with ≥ 20 open restaurants:
- (a) restaurant density per 1,000 population
- (b) avg price range (attribute `RestaurantsPriceRange2`, values 1–4)
- (c) avg weekly operating hours (from `hours` field)
- (d) avg review stars for years 2017–2019

Correlations of median_household_income with each of (a)–(d). Repeat for top-10 highest and top-10 lowest income ZCTAs (ties broken by smallest zcta).

### Spark features / APIs

- `parse_json(col("attributes"))` → `try_variant_get(..., "RestaurantsPriceRange2", "string")` → `try_cast(... as int)`
- `parse_json(col("hours"))` → `variant_explode()` → parse "HH:MM-HH:MM" per day → compute daily hours (handle overnight: if close ≤ open, add 24h)
- `F.corr()` for Pearson correlation
- PartitionFilters for years 2017–2019 in review Parquet

### Experiments & measurements

| Experiment | Config | What to observe |
|---|---|---|
| Default run | 4×(1c,2GB) | Join strategies, partition pruning in `explain()` |
| Config A | 1 exec × 4 cores, 8GB | Timing |
| Config B | 2 exec × 2 cores, 4GB | Timing |
| Config C | 4 exec × 1 core, 2GB | Timing |

### Report contents

- Join strategies and discussion.
- `explain()`: confirm only partitions year=2017, 2018, 2019 are read.
- Count of excluded ZCTAs (negative/zero ACS values).
- Count of ZCTAs with income = 250001 (top-coded).
- Correlation table (full, top-10, bottom-10).
- Timing table for 3 executor configs + discussion.

### ⚠️ Pitfalls

- **Tip 6**: ACS negative values (-666666666, -888888888, -999999999) → filter out. Population 0 → filter out. Keep 250001 and 2499 (censored but valid).
- **Tip 7**: attributes/hours → STRING → `parse_json()` → VARIANT. Hours overnight rule: close ≤ open means next day. "0:0-0:0" = 24 hours.
- **Tip 8**: ANSI mode → `try_variant_get` and `try_cast` for dirty attribute values like "None", "u'free'".
- Canadian postal codes (non-numeric) won't match ZCTA → report how many businesses excluded.

---

## Query 6 — GloVe Word & Category Similarity (10%)

### What to implement

**(a)** For each of ["pizza", "sushi", "coffee", "vegan", "museum"], find the 10 nearest words (cosine similarity), excluding the word itself.

**(b)** Build a vector for each Yelp category = average of GloVe vectors of its words (lowercase, split on non-alpha, drop OOV). For each seed word in (a), find the 5 nearest categories + their business count.

Also: for one seed word, implement ORDER BY similarity DESC LIMIT 10 (without NEAREST BY) and compare physical plans.

### Spark features / APIs

- `vector_cosine_similarity(v1, v2)` — requires `ARRAY<FLOAT>`
- `vector_avg(array_of_vectors)` — for category vector
- `NEAREST BY SIMILARITY` — Tip 5
- `regexp_replace()`, `lower()`, `split()` for tokenizing category names

### Experiments & measurements

| Experiment | Config | What to observe |
|---|---|---|
| NEAREST BY vs ORDER BY LIMIT | 4×(1c,2GB) | Compare physical plans |
| Report: categories without vectors | — | Count of Yelp categories where no word matched GloVe vocabulary |

### Report contents

- Top-10 neighbors table for each seed word.
- Top-5 categories table for each seed word + business counts.
- Plan comparison: NEAREST BY vs ORDER BY LIMIT.
- Count of categories with no vector.
- Commentary: are the nearest categories reasonable?

### ⚠️ Pitfalls

- **Tip 9**: GloVe is lowercase → lowercase category names before splitting.
- Split on non-alphabetic characters: `split(lower(category), "[^a-z]+")`.
- OOV words are silently dropped. If ALL words are OOV, the category gets no vector.
- `ARRAY<FLOAT>` requirement — cast explicitly.

---

## Measurement Protocol (applies to all queries)

1. **Wall-clock timing** around `collect()` (small results) or `write.format("noop")`.
2. **No** `count()` on intermediate DataFrames (optimizer drops columns).
3. **≥ 3 repetitions** per configuration, each in a **new Spark application** (new `SparkSession`, new `spark-submit`).
4. Report **median** and **range** (max − min).
5. **No** `cache()` between compared implementations, except where explicitly requested (Q1).
6. Report **task count per stage**.
7. **Unique `appName`** per experiment → easy identification in History Server.
8. **Event logs** → compress and commit to `event_logs/`.

---

## Deliverables Checklist

- [ ] PDF report: results, hardware, cluster config, repo link + commit hash
- [ ] Git repo:
  - [ ] Source code (src/)
  - [ ] spark-submit scripts with per-experiment configs (scripts/)
  - [ ] Compressed event logs (event_logs/)
  - [ ] NO data files
- [ ] Oral exam preparation: understand every line of code

---

## Next Steps — Commands to Run

> [!IMPORTANT]
> These commands are updated as we progress. Check items off as you complete them.

### Phase 1: Data acquisition

```bash
# 1. Create data directory
mkdir -p ~/advdb/raw && cd ~/advdb/raw

# 2. Download GloVe (862 MB zip → 347 MB for 100d)
wget https://nlp.stanford.edu/data/glove.6B.zip
unzip -j glove.6B.zip glove.6B.100d.txt && rm glove.6B.zip

# 3. Yelp: manually download from https://www.yelp.com/dataset
#    Extract into ~/advdb/raw/:
#    tar -xvf yelp_dataset.tar

# 4. ACS: already in the repo (acs_zcta.csv). Copy to raw:
cp ~/path/to/starsnsparks/acs_zcta.csv ~/advdb/raw/

# 5. Verify line counts
wc -l ~/advdb/raw/yelp_academic_dataset_business.json   # expect 150346
wc -l ~/advdb/raw/yelp_academic_dataset_review.json     # expect 6990280
wc -l ~/advdb/raw/glove.6B.100d.txt                     # expect 400000
wc -l ~/advdb/raw/acs_zcta.csv                          # expect 33775 (header + 33774 data rows)
```

### Phase 2: Cluster startup

```bash
cd ~/path/to/starsnsparks
bash scripts/cluster_start.sh
# Verify: open http://localhost:8080 — should show 1 worker with 8 cores, 8 GB
```

### Phase 3: ETL (Requirement 0)

```bash
# Convert all data to Parquet
bash scripts/run_etl.sh

# Verify Parquet files exist
ls -lh ~/advdb/parquet/business/
ls -lh ~/advdb/parquet/review/    # should have year=XXXX subdirectories
ls -lh ~/advdb/parquet/user/
ls -lh ~/advdb/parquet/glove/
ls -lh ~/advdb/parquet/acs/

# Run format benchmark
bash scripts/run_bench_formats.sh
```

### Phase 4: Queries (one at a time)

```bash
bash scripts/run_q1.sh    # Query 1 — all experiments
bash scripts/run_q2.sh    # Query 2
bash scripts/run_q3.sh    # Query 3
bash scripts/run_q4.sh    # Query 4
bash scripts/run_q5.sh    # Query 5
bash scripts/run_q6.sh    # Query 6
```

### Phase 5: Package event logs

```bash
bash scripts/compress_logs.sh   # compress & copy to event_logs/
git add event_logs/
git commit -m "Add compressed event logs for all experiments"
```
