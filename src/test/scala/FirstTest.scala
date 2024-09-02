package org.larinpaul.sparkdev

import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

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

    // Specifying test data
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-03-01"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )

    // Defining the expected output data
    val expected = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )

    // Creating a test DataFrame using the schema and encoder
    implicit val encoder: Encoder[Row] = Encoders.row(schema)

    // Creating a test DataFrame with schema and encoder
    val testDf = spark.createDataset(testRows)

//    // Calling the unit under test
//    val resultList = Main.highestClosingPricesPerYear(testDf)
//      .collect() // .collect() will return an array of rows

    // Asserting result correctness
    // Comparing actual output with expected output
    val actualRows = Main.highestClosingPricesPerYear(testDf)
      .collect()
    // That's our assertion

    actualRows should contain theSameElementsAs expected
  }


}
