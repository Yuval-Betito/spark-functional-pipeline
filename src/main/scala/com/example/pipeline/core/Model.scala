package com.example.pipeline.core

import scala.util.Try

/**
 * Domain models and pure CSV codecs (no I/O).
 *
 * Defines immutable case classes (`Transaction`, `Product`) and companion objects
 * that provide pure CSV encoders/decoders with functional error handling.
 */
final case class Transaction(
                              /**
                               * @param txnId     unique transaction identifier
                               * @param userId    user identifier
                               * @param productId product identifier
                               * @param quantity  purchased quantity (non-negative)
                               * @param unitPrice price per unit
                               * @param timestamp event time (epoch millis)
                               */
                              txnId: String,
                              userId: String,
                              productId: String,
                              quantity: Int,
                              unitPrice: Double,
                              timestamp: Long
                            ) {

  /**
   * Backwards-compatible alias for the unit price, kept for tests/API stability.
   *
   * @return the unit price value
   */
  def price: Double = unitPrice

  /**
   * Pure per-transaction revenue.
   *
   * @return quantity * unitPrice as Double
   */
  def revenue: Double = quantity.toDouble * unitPrice
}

/**
 * Minimal product dimension used for joining transactions to categories.
 *
 * @param productId product identifier
 * @param category  canonical category label
 */
final case class Product(
                          productId: String,
                          category: String
                        )

object Transaction {

  /** Expected CSV header (used for optional header-skipping). */
  val CsvHeader: String =
    "txnId,userId,productId,quantity,unitPrice,timestamp"

  /**
   * Pure encoder to CSV (no quoting/escaping beyond commas assumption).
   *
   * @param t transaction value to encode
   * @return comma-separated CSV line
   */
  def toCsv(t: Transaction): String =
    s"${t.txnId},${t.userId},${t.productId},${t.quantity},${t.unitPrice},${t.timestamp}"

  /**
   * Lightweight check for the canonical header line.
   *
   * @param line raw CSV line
   * @return true if the line matches the expected header (case-insensitive), else false
   */
  def isHeader(line: String): Boolean =
    line.trim.equalsIgnoreCase(CsvHeader)

  /**
   * Pure CSV parser for a `Transaction` row.
   *
   * Skips the header by returning a `Left(ParseError.BadTransaction(...))`
   * so callers can decide how to handle it (e.g., filter out header `Left`s).
   * Validates arity (6 columns) and numeric fields (`quantity`, `unitPrice`, `timestamp`).
   *
   * @param line raw CSV line (comma separated, no quotes expected)
   * @return `Right(Transaction)` on success, otherwise a `Left(ParseError)`
   */
  def fromCsv(line: String): Either[ParseError, Transaction] = {
    val cols = line.split(",", -1).map(_.trim)

    if (isHeader(line))
      Left(ParseError.BadTransaction(line, "header line (skipped)"))
    else if (cols.length != 6)
      Left(ParseError.BadTransaction(line, s"expected 6 columns, got ${cols.length}"))
    else {
      val Array(txnId, userId, productId, qStr, priceStr, tsStr) = cols

      val qE  = parseInt(qStr).left.map(_ =>
        ParseError.BadTransaction(line, s"quantity is not Int: '$qStr'")
      )
      val pE  = parseDouble(priceStr).left.map(_ =>
        ParseError.BadTransaction(line, s"unitPrice is not Double: '$priceStr'")
      )
      val tsE = parseLong(tsStr).left.map(_ =>
        ParseError.BadTransaction(line, s"timestamp is not Long: '$tsStr'")
      )

      for {
        q  <- qE
        p  <- pE
        ts <- tsE
      } yield Transaction(txnId, userId, productId, q, p, ts)
    }
  }

  // ---------- private pure helpers ----------

  /**
   * Best-effort string → Int with `Either` for functional error handling.
   *
   * @param s input string
   * @return Right(intValue) if parsable, otherwise Left(throwable)
   */
  private def parseInt(s: String): Either[Throwable, Int] =
    Try(s.toInt).toEither

  /**
   * Best-effort string → Long with `Either` for functional error handling.
   *
   * @param s input string
   * @return Right(longValue) if parsable, otherwise Left(throwable)
   */
  private def parseLong(s: String): Either[Throwable, Long] =
    Try(s.toLong).toEither

  /**
   * Best-effort string → Double with `Either` for functional error handling.
   *
   * @param s input string
   * @return Right(doubleValue) if parsable, otherwise Left(throwable)
   */
  private def parseDouble(s: String): Either[Throwable, Double] =
    Try(s.toDouble).toEither
}

object Product {

  /** Expected CSV header for the product dimension. */
  val CsvHeader: String = "productId,category"

  /**
   * Pure encoder to CSV (comma-separated, no quoting).
   *
   * @param p product value to encode
   * @return comma-separated CSV line
   */
  def toCsv(p: Product): String =
    s"${p.productId},${p.category}"

  /**
   * Lightweight check for the canonical header line.
   *
   * @param line raw CSV line
   * @return true if the line matches the expected header (case-insensitive), else false
   */
  def isHeader(line: String): Boolean =
    line.trim.equalsIgnoreCase(CsvHeader)

  /**
   * Pure CSV parser for a `Product` row.
   *
   * Skips the header by returning a `Left(ParseError.BadProduct(...))`.
   * Validates arity (2 columns).
   *
   * @param line raw CSV line (comma separated, no quotes expected)
   * @return `Right(Product)` on success, otherwise a `Left(ParseError)`
   */
  def fromCsv(line: String): Either[ParseError, Product] = {
    val cols = line.split(",", -1).map(_.trim)

    if (isHeader(line))
      Left(ParseError.BadProduct(line, "header line (skipped)"))
    else if (cols.length != 2)
      Left(ParseError.BadProduct(line, s"expected 2 columns, got ${cols.length}"))
    else {
      val Array(productId, category) = cols
      Right(Product(productId, category))
    }
  }
}



