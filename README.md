# Functional Data Processing Pipeline (Scala + Apache Spark)

A complete analytics pipeline in **Scala** + **Apache Spark**, implemented in a **functional** style: pure functions, immutability, higher-order functions, **currying/closures**, combinators, and tail recursion.  
The app reads external CSV files and writes result CSVs.

---

## Quick Start

```bash
# 1) Generate input data (≈20,000 rows by default)
sbt "runMain com.example.pipeline.app.DataGen"

# 2) Run the pipeline (read CSV -> process -> write outputs to ./out)
sbt "runMain com.example.pipeline.app.Main data/transactions.csv data/products.csv out 0.0"

# 3) Run tests (unit + end-to-end)
sbt test
```

On success you’ll see:
```
[INFO] Transactions rows: 10000
```
and CSV outputs under `./out`.

---

## Environment

- JDK 11 (tested with Adoptium 11)
- Scala 2.12.19
- Apache Spark 3.5.1
- sbt (latest)

> Dependencies are managed by sbt; a separate Spark install is **not** required for local runs.

---

## Project Structure

```
src
├─ main
│  └─ scala/com.example.pipeline
│     ├─ app
│     │  ├─ DataGen.scala       # generates external CSV inputs
│     │  ├─ Main.scala          # CLI entry point
│     │  └─ SparkJobs.scala     # read -> filter -> join -> aggregate -> write
│     └─ core
│        ├─ Combinators.scala   # pipe / composeAll, etc.
│        ├─ Errors.scala        # ParseError ADT (Either-based)
│        ├─ Logic.scala         # pure logic (parsing, tail-rec, aggregations)
│        └─ Model.scala         # domain case classes + CSV headers
└─ test
   └─ scala/com.example.pipeline
      ├─ app
      │  └─ EndToEndSpec.scala      # E2E with real Spark (≥10k rows)
      └─ core
         ├─ CombinatorsSpec.scala
         ├─ LogicSpec.scala          # includes explicit currying test
         └─ UnifiedParsingSpec.scala # unified parsing tests (transactions + products)
```

---

## Data Schema

- **products.csv**: `productId,category`
- **transactions.csv**: `txnId,userId,productId,quantity,unitPrice,timestamp`

---

## Pipeline Overview

1. **Read** external CSVs into DataFrame/Dataset.
2. **Filter** transactions by a **curried** predicate (closure capturing a threshold).
3. Compute per-transaction **total**.
4. **Join** with products by `productId`.
5. **Group & aggregate** revenue by `category`.
6. **Write** results as CSV under `./out`.
7. For verification: compute the same revenue in **pure Scala (no Spark)** and write as CSV.

**Spark operations used (≥ 4):**  
`filter`, `withColumn`, `join`, `groupBy` + `agg`, `orderBy`.

**Outputs**
- `out/revenue_by_category/part-*.csv` (Spark result)
- `out/revenue_pure/part-*.csv` (pure functional result)

---

## Functional Programming (exactly 3 documented items)

1) **Combinators & Composition**
    - `composeAll` and `pipe` (see `Combinators.scala`, used in `SparkJobs.scala`).

2) **Currying & Closure**
    - `Logic.minTotalFilter(threshold)(t)` (alias: `totalAtLeast`).
    - Used in the Spark filter as a captured predicate:
      ```scala
      txnsDS.filter(Logic.minTotalFilter(minTotalThreshold) _)
      ```

3) **Tail Recursion**
    - `sumTailRec`, `meanTailRec` in `Logic.scala`.

*(Additionally present but not counted toward the three: functional error handling with `Either[ParseError, A]`, case classes/immutability, and pattern matching.)*

---

## Tests & Documentation

- **Unit tests (core only):** parsing (unified in `UnifiedParsingSpec.scala`), tail-rec helpers, pure aggregation, **currying** (`LogicSpec.scala`).
- **End-to-End test (app):** runs Spark on ≥10k rows and verifies outputs (`app/EndToEndSpec.scala`).
- **ScalaDoc** comments in core and app modules.

Run all tests:
```bash
sbt test
```

---

## CLI Usage

```bash
sbt "runMain com.example.pipeline.app.Main <transactions.csv> <products.csv> <outDir> <minTotal>"
# Example:
sbt "runMain com.example.pipeline.app.Main data/transactions.csv data/products.csv out 0.0"
```

Arguments:
- `<transactions.csv>` — path to the transactions file
- `<products.csv>` — path to the products file
- `<outDir>` — output directory (created/overwritten)
- `<minTotal>` — minimum transaction total (`quantity * unitPrice`)

---


## Notes / Troubleshooting

- Spark on JDK 11 may print “illegal reflective access” warnings — safe to ignore for this assignment.
- Outputs are written with mode `overwrite`. Delete `./out` to start fresh.

