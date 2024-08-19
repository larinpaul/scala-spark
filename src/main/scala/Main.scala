package org.larinpaul.sparkdev

import org.apache.spark.sql.SparkSession // SparkSession is part of the sql package...

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    println("Hello world!")
    println("Hello world!")
    println("We added this VM option in configuration! :)" +
      "--add-exports java.base/sun.nio.ch=ALL-UNNAMED");
    println("We've also now added this VM option as a template :)")

    val spark = SparkSession.builder()
      .appName("scala-spark")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df = spark.read // this returns something called a dataframe
      .option("header", value = true)
      .option("inferSchema", value = true) // this helps infer schema datatypes
      .csv("data/A.csv")

    // dataframes haves actions,
    // for example show() prints out 20 lines by default
    df.show()
    // We can look it up in the Spark documentation as well
    // spark.apache.org/docs/latest/
    // spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/index.html
    // there is also
    // def read: DataFrameReader
    // Returns a DataFrameReader that can be used to read non-streaming data in as a DataFrame
    // let's go to...
    // spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html
    // it has this method:
    // def csv(paths: String*): DataFrane
    // Loads CSV files and returns the result as a DataFrame.
    // ...
    // You can find the CSV-specific options for reading CSV files
    // in Data Source Option in the version you use.
    // if we click on Data Source Option, it will lead us to:
    // spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option

    // Let's explore the schema of the dataframe
    df.printSchema()
    // Datatypes on the Spark website
    // spark.apache.org/docs/latest/sql-ref-datatypes.html
    // printSchema() returns all strings...
    // but there is a way to infer the datatypes correctly... inferSchema option!
    // spark.apache.org/docs/latest/sql-data-sources-csv.html


  }
}
