// test/scala/com/example/pipeline/core/CombinatorsSpec.scala
package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite

class CombinatorsSpec extends AnyFunSuite {
  import Combinators._

  test("pipe + composeAll + mapWhere") {
    val x   = 3.pipe(_ + 1).pipe(_ * 2)
    assert(x == 8)

    val f    = composeAll[Int](List(_ + 1, _ * 2))
    assert(f(3) == 8)

    val out  = mapWhere[Int,Int](_ % 2 == 0)(_ * 10)(List(1,2,3,4))
    assert(out == List(Left(1), Right(20), Left(3), Right(40)))
  }
}

