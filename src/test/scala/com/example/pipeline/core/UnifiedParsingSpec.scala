package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unified parsing tests for both products and transactions CSV rows.
 * This replaces the previous ParsingSpec and ParseProductCsvSpec.
 *
 * Schema (as in Model.scala / DataGen.scala):
 *   products.csv    -> productId,category
 *   transactions.csv-> txnId,userId,productId,quantity,unitPrice,timestamp
 */
final class UnifiedParsingSpec extends AnyFunSuite with Matchers {
  import Logic._

  // ------------------------
  // Products
  // ------------------------

  test("parseProductCsv: header line is rejected (Left)") {
    val header = Product.CsvHeader // "productId,category"
    val res    = parseProductCsv(header)
    res.isLeft shouldBe true
  }

  test("parseProductCsv: happy path") {
    val line = "p-001,Books"
    parseProductCsv(line) match {
      case Right(p) =>
        p.productId shouldBe "p-001"
        p.category  shouldBe "Books"
      case Left(err) =>
        fail(s"Expected Right(Product), got Left($err)")
    }
  }

  test("parseProductCsv: wrong arity -> Left") {
    val line = "p-002" // only one column
    val res  = parseProductCsv(line)
    res.isLeft shouldBe true
  }

  // ------------------------
  // Transactions
  // ------------------------

  test("parseTransactionCsv: header line is rejected (Left)") {
    val header = Transaction.CsvHeader // "txnId,userId,productId,quantity,unitPrice,timestamp"
    val res    = parseTransactionCsv(header)
    res.isLeft shouldBe true
  }

  test("parseTransactionCsv: happy path") {
    val line = "t-001,u-777,p-001,2,19.90,1725678901"
    parseTransactionCsv(line) match {
      case Right(t) =>
        t.txnId     shouldBe "t-001"
        t.userId    shouldBe "u-777"
        t.productId shouldBe "p-001"
        t.quantity  shouldBe 2
        t.unitPrice shouldBe 19.90 +- 0.0001
        t.timestamp shouldBe 1725678901L
      case Left(err) =>
        fail(s"Expected Right(Transaction), got Left($err)")
    }
  }

  test("parseTransactionCsv: trims whitespace") {
    val line = "  t-002 , u-888 , p-010 , 3 , 5.5 , 1725000000  "
    parseTransactionCsv(line) match {
      case Right(t) =>
        t.txnId     shouldBe "t-002"
        t.userId    shouldBe "u-888"
        t.productId shouldBe "p-010"
        t.quantity  shouldBe 3
        t.unitPrice shouldBe 5.5 +- 0.0001
        t.timestamp shouldBe 1725000000L
      case Left(err) =>
        fail(s"Expected Right(Transaction), got Left($err)")
    }
  }

  test("parseTransactionCsv: non-integer quantity -> Left") {
    val line = "t-003,u-999,p-001,notInt,7.0,1725678901"
    parseTransactionCsv(line).isLeft shouldBe true
  }

  test("parseTransactionCsv: negative quantity -> Left") {
    val line = "t-004,u-999,p-001,-1,7.0,1725678901"
    parseTransactionCsv(line).isLeft shouldBe true
  }

  test("parseTransactionCsv: non-double unitPrice -> Left") {
    val line = "t-005,u-999,p-001,1,NaNish,1725678901"
    parseTransactionCsv(line).isLeft shouldBe true
  }

  test("parseTransactionCsv: non-long timestamp -> Left") {
    val line = "t-006,u-999,p-001,1,9.99,notEpoch"
    parseTransactionCsv(line).isLeft shouldBe true
  }

  test("parseTransactionCsv: wrong arity -> Left") {
    val line = "t-007,u-1,p-1,1,9.99" // only 5 columns
    parseTransactionCsv(line).isLeft shouldBe true
  }
}

