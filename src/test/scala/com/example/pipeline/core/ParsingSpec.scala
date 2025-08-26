// test/scala/com/example/pipeline/core/ParsingSpec.scala
package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite

class ParsingSpec extends AnyFunSuite {
  test("parseTxn happy path") {
    val e = Parsing.parseTxn("p1,2,3.5")
    assert(e.exists(t => t.productId=="p1" && t.quantity==2 && t.price==3.5))
  }
  test("parseTxn bad number") {
    assert(Parsing.parseTxn("p1,x,3.5").isLeft)
  }
}
