package org.larinpaul.sparkdev

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite {

  // Instantiate a SparkSession
  private val spark = SparkSession.builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()


  test("add(2, 3) return 5") {
    // Creating a test DataFrame with schema and encoder
    val testRows = Seq(
      Row()
    )
    val testDf = spark.createDataset()
    Main.highestClosingPricesPerYear()
  }


}
