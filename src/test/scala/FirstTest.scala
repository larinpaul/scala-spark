package org.larinpaul.sparkdev

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite {

  // Instantiate a SparkSession
  private val spark = SparkSession.builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()


  test("add(2, 3) return 5") {
    Main.highestClosingPricesPerYear()
  }


}
