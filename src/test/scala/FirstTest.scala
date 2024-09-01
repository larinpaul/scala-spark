package org.larinpaul.sparkdev

import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite {

  test("add(2, 3) return 5") {
    val result = Main.add(2, 3)
    assert(result == 5)
  }

  test("addFake(2, 3) return 5") {
    val result = Main.addFake(2, 3)
    assert(result == 5)
  }


}
