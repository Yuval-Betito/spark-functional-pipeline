package com.example.pipeline.core

import scala.util.Try

/**
 * Pure parsing utilities for CSV lines used by the functional core.
 *
 * All functions here are pure and side-effect free. They return `Either[ParseError, A]`
 * to model validation/parsing failures without throwing exceptions.
 *
 * The parsers are intentionally minimal and consistent with the functional-core design:
 * validation stays local, results are explicit, and no I/O occurs here.
 */
object Parsing {

  /**
   * Parse a CSV line in the format: `"productId,quantity,price"`.
   *
   * Validation rules:
   *  - Exactly 3 columns are required.
   *  - `quantity` must parse to `Int`.
   *  - `price` must parse to `Double`.
   *
   * On success, constructs a `Transaction` where fields that do not appear in the CSV
   * (`txnId`, `userId`, `timestamp`) are set to neutral placeholders, as they are
   * irrelevant for this minimal parsing use-case.
   *
   * @param line a single CSV line (non-null)
   * @return `Right(Transaction)` on success; `Left(ParseError)` on validation or parsing error
   */
  def parseTxn(line: String): Either[ParseError, Transaction] = {
    val cols = line.split(",", -1).map(_.trim)
    if (cols.length != 3) {
      Left(ParseError.BadTransaction(line, s"expected 3 columns, got ${cols.length}"))
    } else {
      val Array(productId, quantityStr, priceStr) = cols

      val quantityEither: Either[ParseError, Int] =
        Try(quantityStr.toInt).toEither.left.map { _ =>
          ParseError.BadTransaction(line, s"quantity is not Int: '$quantityStr'")
        }

      val priceEither: Either[ParseError, Double] =
        Try(priceStr.toDouble).toEither.left.map { _ =>
          ParseError.BadTransaction(line, s"price is not Double: '$priceStr'")
        }

      for {
        q <- quantityEither
        p <- priceEither
      } yield Transaction(
        txnId     = "",   // placeholder (not present in this CSV)
        userId    = "",   // placeholder (not present in this CSV)
        productId = productId,
        quantity  = q,
        unitPrice = p,    // `price` in CSV maps to `unitPrice` in the domain model
        timestamp = 0L    // placeholder (not present in this CSV)
      )
    }
  }

  /**
   * Parse a CSV line in the format: `"productId,category"`.
   *
   * Delegates to the domain model constructor (`Product.fromCsv`) to keep validation
   * centralized, still returning `Either` rather than throwing.
   *
   * @param line a single CSV line (non-null)
   * @return `Right(Product)` on success; `Left(ParseError)` on validation or parsing error
   */
  def parseProduct(line: String): Either[ParseError, Product] =
    Product.fromCsv(line)
}




