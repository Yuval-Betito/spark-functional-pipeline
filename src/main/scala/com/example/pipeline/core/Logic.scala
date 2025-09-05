package com.example.pipeline.core

import scala.annotation.tailrec
import scala.util.Try
import com.example.pipeline.core.ParseError._

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
   * Parse a CSV line into a [[Transaction]].
   *
   * Expected schema (6 columns): txnId,userId,productId,quantity,unitPrice,timestamp
   *
   * @param line raw CSV line
   * @return Right(Transaction) on success; Left(ParseError) on failure
   */
  def parseTransactionCsv(line: String): Either[ParseError, Transaction] = {
    val parts = line.split(",", -1).map(_.trim).toList
    parts match {
      case txnId :: userId :: productId :: qtyStr :: priceStr :: tsStr :: Nil =>
        for {
          q  <- Try(qtyStr.toInt).toEither.left.map(_ => BadTransaction(line, s"quantity=$qtyStr"))
          up <- Try(priceStr.toDouble).toEither.left.map(_ => BadTransaction(line, s"unitPrice=$priceStr"))
          ts <- Try(tsStr.toLong).toEither.left.map(_ => BadTransaction(line, s"timestamp=$tsStr"))
        } yield Transaction(txnId, userId, productId, q, up, ts)

      case _ =>
        Left(BadTransaction(line, "wrong number of columns"))
    }
  }

  /**
   * Parse a CSV line into a [[Product]].
   *
   * Expected schema (2 columns): productId,category
   *
   * @param line raw CSV line
   * @return Right(Product) on success; Left(ParseError) on failure
   */
  def parseProductCsv(line: String): Either[ParseError, Product] = {
    val parts = line.split(",", -1).map(_.trim).toList
    parts match {
      case pid :: cat :: Nil => Right(Product(pid, cat))
      case _                 => Left(BadProduct(line, "wrong number of columns"))
    }
  }

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




