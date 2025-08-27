package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for CSV parsing using the Logic.* parsers.
 * Formats:
 *  - Transaction: "txnId,userId,productId,quantity,unitPrice,timestamp"
 *  - Product:     "productId,category"
 */
final class ParsingSpec extends AnyFunSuite with Matchers {

  test("parseTransactionCsv should parse a valid transaction line") {
    val line = "t1,u1,p42,3,19.99,1711300000000"
    Logic.parseTransactionCsv(line) match {
      case Right(t) =>
        t.txnId     shouldBe "t1"
        t.userId    shouldBe "u1"
        t.productId shouldBe "p42"
        t.quantity  shouldBe 3
        t.unitPrice shouldBe 19.99 +- 1e-9
        t.timestamp shouldBe 1711300000000L
      case Left(err) =>
        fail(s"Expected Right, got Left($err)")
    }
  }

  test("parseTransactionCsv should fail on malformed line") {
    val bad = "t1,u1,p42,not_a_number,19.99,1711300000000"
    Logic.parseTransactionCsv(bad).isLeft shouldBe true
  }

  test("parseProductCsv should parse a valid product line") {
    val good = "p7,Toys"
    Logic.parseProductCsv(good) match {
      case Right(p) =>
        p.productId shouldBe "p7"
        p.category  shouldBe "Toys"
      case Left(err) =>
        fail(s"Expected Right, got Left($err)")
    }
  }
}


