package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.example.pipeline.core.ParseError._

class LogicSpec extends AnyFunSuite with Matchers {

  test("parseTransactionCsv parses valid line") {
    val line = "t1,u1,p1,2,10.5,1711300000000"
    val res  = Logic.parseTransactionCsv(line)
    res.isRight shouldBe true
    val t = res.toOption.get
    t.txnId shouldBe "t1"
    t.quantity shouldBe 2
    t.unitPrice shouldBe 10.5
  }

  test("parseTransactionCsv fails on bad quantity") {
    val line = "t1,u1,p1,ZZ,10.5,1711300000000"
    Logic.parseTransactionCsv(line).left.get shouldBe a [BadTransaction]
  }

  test("parseProductCsv parses valid line") {
    val line = "p1,Books"
    Logic.parseProductCsv(line) shouldBe Right(Product("p1","Books"))
  }

  test("sumTailRec and meanTailRec work") {
    Logic.sumTailRec(List(1.0, 2.0, 3.0)) shouldBe 6.0
    Logic.meanTailRec(List(1.0, 2.0, 3.0)) shouldBe Some(2.0)
    Logic.meanTailRec(Nil) shouldBe None
  }

  test("revenueByCategory groups correctly") {
    val pBooks = Product("p1","Books")
    val pToys  = Product("p2","Toys")
    val t1 = Transaction("t1","u1","p1", 2, 10.0, 1L) // 20
    val t2 = Transaction("t2","u2","p2", 1, 50.0, 1L) // 50
    val t3 = Transaction("t3","u3","p1", 3,  5.0, 1L) // 15

    val m = Logic.revenueByCategory(List((t1,pBooks),(t2,pToys),(t3,pBooks)))
    m("Books") shouldBe 35.0
    m("Toys")  shouldBe 50.0
  }
}
