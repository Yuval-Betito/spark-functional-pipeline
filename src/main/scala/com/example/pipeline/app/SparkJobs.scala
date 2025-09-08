package com.example.pipeline.app

import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import com.example.pipeline.core._
import com.example.pipeline.core.Combinators._

/**
 * Entry points for Spark jobs used by the application and tests.
 *
 * Responsibilities:
 *  - Load input CSVs into well-typed DataFrames
 *  - Run the end-to-end pipeline (filter → enrich → join → aggregate)
 *  - Persist both Spark-based and pure domain results
 */
object SparkJobs {

  /**
   * Load the transactions CSV into a DataFrame with proper types.
   *
   * Expected header columns:
   *   txnId, userId, productId, quantity, unitPrice, timestamp
   *
   * @param spark Active [[SparkSession]].
   * @param path  Path to the transactions CSV (local/HDFS/S3 URI).
   * @return A DataFrame with columns:
   *         txnId (string), userId (string), productId (string),
   *         quantity (int), unitPrice (double), timestamp (long).
   */
  def loadTransactionsDF(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").csv(path)
      .select(
        F.col("txnId"),
        F.col("userId"),
        F.col("productId"),
        F.col("quantity").cast("int"),
        F.col("unitPrice").cast("double"),
        F.col("timestamp").cast("long")
      )

  /**
   * Load the products CSV into a DataFrame with proper types.
   *
   * Expected header columns:
   *   productId, category
   */
  def loadProductsDF(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").csv(path)
      .select(F.col("productId"), F.col("category"))

  /**
   * End-to-end pipeline.
   *
   * @param spark              active [[SparkSession]]
   * @param txnsPath           path to transactions.csv
   * @param productsPath       path to products.csv
   * @param outDir             output directory for results
   * @param minTotalThreshold  minimal transaction total (quantity*unitPrice)
   * @param minRows            minimal number of rows required (default 10k)
   */
  def runPipeline(
                   spark: SparkSession,
                   txnsPath: String,
                   productsPath: String,
                   outDir: String,
                   minTotalThreshold: Double,
                   minRows: Long = 10000L
                 ): Unit = {

    import spark.implicits._

    // 1) Load inputs
    val txnsDF  = loadTransactionsDF(spark, txnsPath)
    val prodsDF = loadProductsDF(spark, productsPath)

    // 2) Enforce minimal input size (configurable for tests)
    val totalRows = txnsDF.count()
    require(totalRows >= minRows, s"Dataset must have >= $minRows rows, got " + totalRows)
    println(s"[INFO] Transactions rows: " + totalRows)

    // 3) Closure-based filter on a typed Dataset (currying already in use)
    val txnsDS     = txnsDF.as[Transaction]
    val filteredDS = txnsDS.filter(Logic.minTotalFilter(minTotalThreshold) _)

    // 4) Compute totals + a tiny demo pipeline using combinators
    val withTotalDF = filteredDS.toDF().withColumn(
      "total",
      F.col("quantity") * F.col("unitPrice")
    )

    /** Spec: Compose functions using combinators */
    val keepNonNegative: DataFrame => DataFrame =
      df => df.filter(F.col("total") >= 0)

    val addDiscount2pct: DataFrame => DataFrame =
      df => df.withColumn("discounted_total", F.col("total") * F.lit(0.98))

    val miniPipeline: DataFrame => DataFrame =
      composeAll(List(keepNonNegative, addDiscount2pct))

    val stagedDF = withTotalDF.pipe(miniPipeline)

    // Join with products
    val joined = stagedDF.join(prodsDF, Seq("productId"), "left")

    // Aggregation
    val revenueByCategory = joined
      .groupBy(F.col("category"))
      .agg(
        F.count("*").as("num_txn"),
        F.sum("total").as("revenue")
      )
      .orderBy(F.desc("revenue"))

    // 5) Write Spark result
    revenueByCategory
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"$outDir/revenue_by_category")

    // Convert to domain and compute an additional pure result
    val pairs: List[(Transaction, Product)] =
      joined
        .select("txnId","userId","productId","quantity","unitPrice","timestamp","category")
        .as[(String,String,String,Int,Double,Long,String)]
        .collect()
        .toList
        .map { case (txnId, userId, pid, q, up, ts, cat) =>
          val t = Transaction(txnId, userId, pid, q, up, ts)
          val p = Product(pid, Option(cat).getOrElse("Unknown"))
          (t, p)
        }

    val pureMap: Map[String, Double] = Logic.revenueByCategory(pairs)
    val pureDf = pureMap.toSeq.toDF("category","revenue_pure")

    pureDf
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"$outDir/revenue_pure")
  }
}








