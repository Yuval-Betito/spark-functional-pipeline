package com.example.pipeline.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for custom functional combinators used by the project.
 *
 * Covered:
 *  - [[Combinators.pipe]]: value-threading helper (x.pipe(f) == f(x)).
 *  - [[Combinators.composeAll]]: left-to-right composition of a list of functions.
 *  - [[Combinators.mapWhere]]: selective mapping with predicate → Either partitioning.
 *
 * Rationale:
 * These tests ensure composability and functional style are correct and
 * demonstrable for the course requirements ("Compose functions using combinators").
 */
final class CombinatorsSpec extends AnyFunSuite with Matchers {
  import Combinators._

  test("pipe + composeAll + mapWhere") {
    val piped = 3.pipe(_ + 1).pipe(_ * 2)
    piped shouldBe 8

    val f = composeAll[Int](List(_ + 1, _ * 2))
    f(3) shouldBe 8

    val out = mapWhere[Int, Int](_ % 2 == 0)(_ * 10)(List(1, 2, 3, 4))
    out shouldBe List(Left(1), Right(20), Left(3), Right(40))
  }
}


