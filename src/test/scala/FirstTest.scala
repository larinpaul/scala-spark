package org.larinpaul.sparkdev

import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite {

  // Instantiate a SparkSession
  private val spark = SparkSession.builder()
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  // Defining the schema
  private val schema = StructType(Seq(
    StructField("date", DateType, nullable = true),
    StructField("open", DoubleType, nullable = true),
    StructField("close", DoubleType, nullable = true)
  ))

  test("add(2, 3) return 5") {
    // Creating a test DataFrame with schema and encoder
    val testRows = Seq(
      Row()
    )
    val testDf = spark.createDataset()
    Main.highestClosingPricesPerYear()
  }


}
