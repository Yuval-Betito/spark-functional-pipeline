package com.example.pipeline.core

import scala.annotation.tailrec

/**
 * Pure functional logic only (no I/O and no Spark).
 *
 * This module centralizes validation and transformations:
 *  - CSV parsing returns Either[ParseError, A] (functional error handling).
 *  - Higher-order/currying example: [[minTotalFilter]].
 *  - Tail-recursive utilities: [[sumTailRec]] and [[meanTailRec]].
 *  - Pure business logic: [[revenueByCategory]].
 */
object Logic {

  // ========= Parsing (CSV) =========

  /**
   * Parse a CSV line into a [[Transaction]] using the canonical domain parser.
   *
   * Expected schema (6 columns): txnId,userId,productId,quantity,unitPrice,timestamp
   *
   * Delegates to [[Transaction.fromCsv]] to keep a single source of truth.
   */
  def parseTransactionCsv(line: String): Either[ParseError, Transaction] =
    Transaction.fromCsv(line)

  /**
   * Parse a CSV line into a [[Product]] using the canonical domain parser.
   *
   * Expected schema (2 columns): productId,category
   *
   * Delegates to [[Product.fromCsv]] to keep a single source of truth.
   */
  def parseProductCsv(line: String): Either[ParseError, Product] =
    Product.fromCsv(line)

  // ========= Higher-order + Currying =========

  /**
   * Curried higher-order predicate that keeps transactions whose total
   * amount (quantity * unitPrice) meets or exceeds a threshold.
   *
   * @param threshold minimal total amount
   * @return a predicate Transaction => Boolean capturing the threshold (closure)
   */
  def minTotalFilter(threshold: Double)(t: Transaction): Boolean =
    t.quantity * t.unitPrice >= threshold

  /** Alias for the same curried predicate, kept for clarity in docs/tests. */
  def totalAtLeast(threshold: Double)(t: Transaction): Boolean =
    minTotalFilter(threshold)(t)

  // ========= Tail Recursion =========

  /**
   * Tail-recursive sum over a list of doubles.
   *
   * @param xs numbers to sum
   * @param acc running accumulator (default 0.0)
   * @return sum(xs)
   */
  @tailrec
  def sumTailRec(xs: List[Double], acc: Double = 0.0): Double = xs match {
    case Nil    => acc
    case h :: t => sumTailRec(t, acc + h)
  }

  /**
   * Mean computed via a tail-recursive inner loop (signature unchanged).
   *
   * @param xs numbers (may be empty)
   * @return Some(mean) or None when xs is empty
   */
  def meanTailRec(xs: List[Double]): Option[Double] = {
    @tailrec
    def loop(ys: List[Double], acc: Double, n: Int): (Double, Int) = ys match {
      case h :: t => loop(t, acc + h, n + 1)
      case Nil    => (acc, n)
    }
    val (sum, n) = loop(xs, 0.0, 0)
    if (n == 0) None else Some(sum / n.toDouble)
  }

  // ========= Composition Example =========

  /**
   * Normalize values around their mean: xi ↦ (xi - mean(xs)).
   *
   * @param xs input values
   * @return normalized values, or xs unchanged when empty
   */
  def normalizeAroundMean(xs: List[Double]): List[Double] =
    meanTailRec(xs).map(m => xs.map(_ - m)).getOrElse(xs)

  // ========= Business Logic (Pure) =========

  /**
   * Compute total revenue per category from (Transaction, Product) pairs.
   * Pure, in-memory implementation (no Spark).
   *
   * @param pairs joined records (transaction with its product/category)
   * @return map from category to total revenue
   */
  def revenueByCategory(pairs: List[(Transaction, Product)]): Map[String, Double] = {
    val grouped: Map[String, List[(Transaction, Product)]] =
      pairs.groupBy { case (_, p) => p.category }

    grouped.map { case (cat, lst) =>
      val revenue = lst.map { case (t, _) => t.quantity * t.unitPrice }.sum
      cat -> revenue
    }
  }
}



