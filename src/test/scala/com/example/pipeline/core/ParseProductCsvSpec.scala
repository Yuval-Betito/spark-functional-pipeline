package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for parsing Product CSV lines.
 * English-only comments per course code style.
 */
final class ParseProductCsvSpec extends AnyFunSuite with Matchers {

  test("parseProductCsv should parse a valid product line") {
    val line = "p1,Books"                 // products.csv has 2 columns: productId,category
    val res  = Logic.parseProductCsv(line)

    res match {
      case Right(p) =>
        p.productId shouldBe "p1"        // <-- field name is productId (not id)
        p.category  shouldBe "Books"
      case Left(err) =>
        fail(s"Expected Right, got Left($err)")
    }
  }

  test("parseProductCsv should fail on malformed line") {
    val bad = "p1"                        // missing category
    Logic.parseProductCsv(bad).isLeft shouldBe true
  }
}




