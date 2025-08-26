package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite

class ParseProductCsvSpec extends AnyFunSuite {
  test("parseProductCsv fails on wrong columns count") {
    val bad = "p1,Books,EXTRA"
    val res = Logic.parseProductCsv(bad)  // <<< היה Parsing.parseProductCsv
    assert(res.isLeft)                    // זה יעבוד כי Either ב-2.12 כולל isLeft
  }
}

