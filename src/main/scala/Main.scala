//package org.larinpaul.sparkdev
//
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.{SparkSession, functions}
//import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit, row_number, year}
//import org.apache.spark.sql.types.StringType // SparkSession is part of the sql package...
//
//object Main {
//  def main(args: Array[String]): Unit = {
//    println("Hello world!")
//    println("Hello world!")
//    println("Hello world!")
//    println("We added this VM option in configuration! :)" +
//      "--add-exports java.base/sun.nio.ch=ALL-UNNAMED");
//    println("We've also now added this VM option as a template :)")
//
//    val spark = SparkSession.builder()
//      .appName("scala-spark")
//      .master("local[*]")
//      .config("spark.driver.bindAddress", "127.0.0.1")
//      .getOrCreate()
//
//    val df = spark.read // this returns something called a dataframe
//      .option("header", value = true)
//      .option("inferSchema", value = true) // this helps infer schema datatypes
//      .csv("data/A.csv")
//
//    // dataframes haves actions,
//    // for example show() prints out 20 lines by default
//    df.show()
//    // We can look it up in the Spark documentation as well
//    // spark.apache.org/docs/latest/
//    // spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/index.html
//    // there is also
//    // def read: DataFrameReader
//    // Returns a DataFrameReader that can be used to read non-streaming data in as a DataFrame
//    // let's go to...
//    // spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html
//    // it has this method:
//    // def csv(paths: String*): DataFrane
//    // Loads CSV files and returns the result as a DataFrame.
//    // ...
//    // You can find the CSV-specific options for reading CSV files
//    // in Data Source Option in the version you use.
//    // if we click on Data Source Option, it will lead us to:
//    // spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option
//
//    // Let's explore the schema of the dataframe
//    df.printSchema()
//    // Datatypes on the Spark website
//    // spark.apache.org/docs/latest/sql-ref-datatypes.html
//    // printSchema() returns all strings...
//    // but there is a way to infer the datatypes correctly... inferSchema option!
//    // spark.apache.org/docs/latest/sql-data-sources-csv.html
//
//
//    // Part 4: The Dataset API
//    // - Dataset is the main abstraction introduced by Spark SQL
//    // - Spark SQL is an abstraction over Spark core's RDDs
//    // - We'll talk about the Spark architecture and execution model later
//    // - The Dataset API defines a DSL
//    // (domain-specific language, declarative, not using Scala functions)
//    // - That's how we tell Spark what to do
//    // - Inspect Spark API within IDE
//    // `type DataFrame = Dataset[Row]`
//    // - `Row` is a generic object (untyped view)
//
//    // Part 5: DSL (i) Referencing columns
//    // - Mostly when using the API, we work with Columns `col("a") + 5`
//    // - Ways of referencing columns: String, apply, col, $ (implicits)
//    // - Not necessarily bound to DataFrame
//
//    df.select("Date", "Open", "Close").show()
//    val column0 = df("Date")
//    col("Date")
//    import spark.implicits._
//    $"Date" // $ operator returns a column
//
//    println("Showing again...")
//    df.select(col("Date"), $"Open", df("Close")).show()
//
//    // this will work took
//    df.select(column0, $"Open", df("Close")).show()
//
//    // this will not work because you can't mix up data types and provide only a string here and others as other formats
////    df.select("Date", $"Open", df("Close")).show()
//
//    // Part 6: DSL (ii) Column functions
//
//    // - The `Column` class
//    // - Functions on columns: `===`, `cast`, `<`, `+`
//    // - Reading the reference
//
//    val column = df("Open")
//    val newColumn = column.plus(2.0) // has all the values increased by 2
//    val newColumn1 = column + (2.0)
//    val columnString = column.cast(StringType) // (org.apache.spark.sql.types) SpringType.type
//
//    df.select(column, newColumn, columnString).show()
//    // we can also do this:
//    df.select(column, newColumn, columnString)
//      .filter(newColumn > 2.0)
//    // or we can even compare two columns
//    df.select(column, newColumn, columnString)
//      .filter(newColumn > 2.0)
//      .filter(newColumn > column)
//      .show() // don't forget to print this out by using .show() ! :)
//    // You can look up all such methods in the IDE,
//    // or on Spark website, Spark 3.5.0 ScalaDoc // sql // Column
//    // for equality check between two columns
//    // you need to use === because == is used by Scala itself
//
//    df.select(column, newColumn, columnString)
//      .filter(newColumn > 2.0)
//      .filter(newColumn > column)
////      .filter(newColumn == ) // this won't work because it compares two objects...
//      .filter(newColumn === column) // it will give us an empty dataframe :)
//      .show()
//
//    // By the way, if you transform a column, you will see the name change:
//    val newColumn2 = column + 2.0
//    // [Open][Open + 2.0][Open]
//    // to give it a more readable name, we can use a method from the Column
//    val newColumnAliased = (column + 2.0).as("OpenIncreasedBy2")
//    val columnStringAliased = column.cast(StringType).as("OpenAsString")
//
//
//    // Part 7: DSL (iii) `sql.functions`
//    // - `col`, `lit`, `concat`
//
//    // Documentation:
//    // spark.apache.org/docs/latest/api/scala/apache/spark/sql/function$.html
//    // for example:
//    // def lit(literal: Any): Column
//    // Creates a Column of literal value
//
//    val litColumn = lit(2.0)
//    val newColumnString = functions.concat(columnString, lit("Hello World"))
//
//    df.select(column, newColumn, columnString, newColumnString)
//      .show(truncate = false)
//
//
//    // Part 8: DSL: (iv) Expressions
//
//    // - We have explored the `Column` class and the `org.apache.spark.sql.functions
//    // - There is another way to transform columns (which I do not recommend using)
//    // - We can also write SQL expressions as strings, which will be interpreted at runtime (no compiler safety)
//    // - [SQL built-in functions reference] (https://spark.apache.org/docs/latest/api/sql/index.html)
//
//    val timestampFromExpression = expr("cast(current_timestamp() as string) as timestampExpression")
//    val timestampFromFunctions = current_timestamp().cast(StringType).as("timestampFunctions")
//
//    df.select(timestampFromExpression, timestampFromFunctions).show()
//
//    // Which functions  are available for these SQL expressions?
//    // They are called SQL built-ins
//    // spark.apache.org/docs/latest/api/sql/index.html
//    // spark.apache.org/docs/latest/api/sql/#current_timestamt
//
//    df.selectExpr("cast(Date as string)", "Open + 1.0", "current_timestamp()").show()
//
//    df.createTempView("df")
//    spark.sql("select * from df").show()
//
//
//    // Part 9: DSL: (v) Rename columns, varargs, withColumn, filter
//
//    // - Rename all columns to be of camel case format
//    // - Add a column containing the diff between `open` and `close`
//    // - Filter to days when the `close` price was more than 10% higher than the open price
//
////    df.withColumnRenamed("Open", "open")
////      .withColumnRenamed("Close", "close") // But there is a smarter way to do this...
//
//    val renameColumns = List(
//      col("Date").as("date"),
//      col("Open").as("open"),
//      col("High").as("high"),
//      col("Low").as("low"),
//      col("Close").as("close"),
//      col("Adj Close").as("adjClose"),
//      col("Volume").as("volume")
//    )
//
//    // Let's use vargarg splice
//    df.select(renameColumns: _*).show()
//
//    // df.columns.map(c => c.toLowerCase())
//    df.select(df.columns.map(c => col(c).as(c.toLowerCase())): _*).show()
//
//    // Add a column containing the diff between `open` and `close`
//    val stockData = df.select(renameColumns: _*)
//      .withColumn("diff", col("close") - col("open"))
//
//    stockData.show()
//
//    // Filter to days when the `close` price was more than 10% higher than the open price
//    val stockDataFilter = df.select(renameColumns: _*)
//      .withColumn("diff", col("close") - col("open"))
//      .filter(col("close") > col("open") * 1.1)
//
//    stockDataFilter.show()
//
//
//    // Part 10: Concept (i) What is Spark?
//
//    // - Why does it take so long? Why do we need to bind to a port?
//    // - Spark is a distributed processing engine: Our code can run locally,
//    // or on dozens or even hundreds of machines
//    // - (BGs of data or Billions of rows)
//    // - Usually used as processing engine on data lakes
//    // (file-based large-scale data stores);
//    // It's not a database
//    // - Master-Slave architecture: Driver does planning of work
//    // and assigns tasks to workers (declarative, SQL-like API)
//    // - Too much overhead for using with small csv files like on our example
//    // - However, that's how we develop code
//
//
//    // Part 11: DSL (vi) Sort, Group, Aggregate
//
//    // - Until now, we have a good understanding how the API is organized
//    // - Select, transform, rename columns (transforming DataFrame on each row)
//    // - In this video: Working with multiple rows in a DataFrame
//    // - Sort on one or multiple columns (asc/desc)
//    // - GroupBy one or multiple columns and applying an aggregation on groups
//    // - Available aggregation functions can be found in `sql.functions`
//    // (for some exists a shorthand `count`, `sum`)
//    // Assignment: Average and highest closing prices per year,
//    // sorted with the highest prices first
//
//    // $ is an implicit
//    import spark.implicits._
//    stockData
//      .groupBy(year($"date").as("year"))
//      .agg(functions.max($"close").as("maxClose"), functions.avg($"close").as("avgClose"))
//      .sort($"maxClose".desc)
//      .show()
//
//    // Using shorthands for aggregations
//    stockData
//      .groupBy(year($"date").as("year"))
//      .max("close", "high")
//      .show()
//
//
//    // Part 12: DSL (vii) Window functions
//
//    // - With `groupBy` we could only retrieve the grouping columns
//    // and the aggregation column (`year`, `maxClose`)
//    // - What if we wanted to see the entire record of the days
//    // with the highest closing prices (e.g. `open`, `highest` values)?
//    // - Explore windows functions as construct to apply a partitioned view
//    // and allowing arbitrary transformations on the grouped data
//    // - Assignment: Find rows of the highest closing price in each year,
//    // sorted with the highest closing prices first
//
//    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
//    stockData
//      .withColumn("rank", row_number().over(window))
//      .filter($"rank" === 1)
//      .sort($"close".desc)
//      .show
//
//
//    // Part 13: Concepts (ii): Partitions, AST, optimization, lazy evaluation
//
//    /// Partitioning
//    // - How can Spark distribute our workload among many machines?
//    // we have to divide the work after all ...
//    // - Let's look at a simple example: Sum all values from one column -
//    // how would you do it?
//    // - Partitioning = Level of parallelism; we aim for equal sized // Level of parallelism = number of partitions
//    // but ... often not possible and one major issue with performance
//    // - There will be one task for processing each partition;
//    // The task ... scheduled on the executors by the driver
//
//    /// AST, logical plan & the Catalyst
//    // - Spark SQL provides us with a fully declarative & structured API
//    // - Meaning it defines operators which we can use to tell Spark
//    // how the resul can be derived form the input (`withColumn`, `groupBy`, `count`)
//    // - Spark SQL is also called the structured API as it deals with structured data
//    // as Datasets have a schema
//    // - Therefore, Spark knows what's in the columns of a DataFrame
//    // - By calling the API, we chain these operators, while Spark assembles
//    // an abstract internal representation: The AST (abstract syntax tree)
//    // - The AST is in a way the computation plan of the result form the input;
//    // it describes how each intermediate Dataset can be derived from its parents
//    // - The AST represents our query in a logical form; therefore,
//    // it's called the logical plan
//    // - As the API is used only to assemble an internal logical representation, it
//    // does not matter which API we use (Scala, Python, R, SQL expressions)
//    // - SparkSQL also has implemented a query optimizer (the Catalyst)
//    // the logical plan and applies general optimization on it
//    // - The result is an optimized logical plan
//
//
//    val stockDataToo = df.select(renameColumns: _*)
//
//    import spark.implicits._
//    val windowToo = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
//    stockDataToo
//      .withColumn("rank", row_number().over(windowToo))
//      .filter($"rank" === 1)
//      .sort($"close".desc)
//      .explain(extended = true)
//
////    24/08/30 21:42:47 INFO FileSourceStrategy: Post-Scan Filters:
////    == Parsed Logical Plan ==
////    'Sort ['close DESC NULLS LAST], true
////    +- Filter (rank#653 = 1)
////    +- Project [date#291, open#292, high#293, low#294, close#295, adjClose#296, volume#297, rank#653]
////    +- Project [date#291, open#292, high#293, low#294, close#295, adjClose#296, volume#297, rank#653, rank#653]
////    +- Window [row_number() windowspecdefinition(year(date#291), close#295 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#653], [year(date#291)], [close#295 DESC NULLS LAST]
////    +- Project [date#291, open#292, high#293, low#294, close#295, adjClose#296, volume#297]
////    +- Project [Date#17 AS date#291, Open#18 AS open#292, High#19 AS high#293, Low#20 AS low#294, Close#21 AS close#295, Adj Close#22 AS adjClose#296, Volume#23 AS volume#297]
////    +- Relation [Date#17,Open#18,High#19,Low#20,Close#21,Adj Close#22,Volume#23] csv
////
////    == Analyzed Logical Plan ==
////    date: date, open: double, high: double, low: double, close: double, adjClose: double, volume: int, rank: int
////    Sort [close#295 DESC NULLS LAST], true
////    +- Filter (rank#653 = 1)
////    +- Project [date#291, open#292, high#293, low#294, close#295, adjClose#296, volume#297, rank#653]
////    +- Project [date#291, open#292, high#293, low#294, close#295, adjClose#296, volume#297, rank#653, rank#653]
////    +- Window [row_number() windowspecdefinition(year(date#291), close#295 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#653], [year(date#291)], [close#295 DESC NULLS LAST]
////    +- Project [date#291, open#292, high#293, low#294, close#295, adjClose#296, volume#297]
////    +- Project [Date#17 AS date#291, Open#18 AS open#292, High#19 AS high#293, Low#20 AS low#294, Close#21 AS close#295, Adj Close#22 AS adjClose#296, Volume#23 AS volume#297]
////    +- Relation [Date#17,Open#18,High#19,Low#20,Close#21,Adj Close#22,Volume#23] csv
////
////    == Optimized Logical Plan ==
////    Sort [close#295 DESC NULLS LAST], true
////    +- Filter (rank#653 = 1)
////    +- Window [row_number() windowspecdefinition(year(date#291), close#295 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#653], [year(date#291)], [close#295 DESC NULLS LAST]
////    +- WindowGroupLimit [year(date#291)], [close#295 DESC NULLS LAST], row_number(), 1
////    +- Project [Date#17 AS date#291, Open#18 AS open#292, High#19 AS high#293, Low#20 AS low#294, Close#21 AS close#295, Adj Close#22 AS adjClose#296, Volume#23 AS volume#297]
////    +- Relation [Date#17,Open#18,High#19,Low#20,Close#21,Adj Close#22,Volume#23] csv
////
////    == Physical Plan ==
////      AdaptiveSparkPlan isFinalPlan=false
////    +- Sort [close#295 DESC NULLS LAST], true, 0
////    +- Exchange rangepartitioning(close#295 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=437]
////    +- Filter (rank#653 = 1)
////    +- Window [row_number() windowspecdefinition(year(date#291), close#295 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS rank#653], [year(date#291)], [close#295 DESC NULLS LAST]
////    +- WindowGroupLimit [year(date#291)], [close#295 DESC NULLS LAST], row_number(), 1, Final
////    +- Sort [year(date#291) ASC NULLS FIRST, close#295 DESC NULLS LAST], false, 0
////    +- Exchange hashpartitioning(year(date#291), 200), ENSURE_REQUIREMENTS, [plan_id=431]
////    +- WindowGroupLimit [year(date#291)], [close#295 DESC NULLS LAST], row_number(), 1, Partial
////    +- Sort [year(date#291) ASC NULLS FIRST, close#295 DESC NULLS LAST], false, 0
////    +- Project [Date#17 AS date#291, Open#18 AS open#292, High#19 AS high#293, Low#20 AS low#294, Close#21 AS close#295, Adj Close#22 AS adjClose#296, Volume#23 AS volume#297]
////    +- FileScan csv [Date#17,Open#18,High#19,Low#20,Close#21,Adj Close#22,Volume#23] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/C:/projects/2024/scala/scala-spark/scala-spark/data/A.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<Date:date,Open:double,High:double,Low:double,Close:double,Adj Close:double,Volume:int>
//
//    /// Lazy evaluations // Lazy evaluation is when an expression is delayed until its value is actually needed
//    // - There are transformations and actions
//    // - A transformation simply adds a step to the AST without doing anything more
//    // - An action in turn is called on the result Dataset
//    // and actually triggers the calculation of the result by executing the AST
//    // 00
//
//
//    // Part 14: Tests (i) Why do we need tests?
//
//    // - Do you know whether the result from our previous assignment was correct?
//    // - I don't, but I hope ;) (is that professional?)
//    // - So we could gain certainty for now by inspecting the csv dataset
//    // and check against our result
//    // - What if the code or the data changes? Do it again? How many times?
//    // (This is called manual testing or an engineer's nightmare)
//    // - High-quality software = Composition of small well-functioning & tested units
//    // - What's a unit test?
//    // - Automated way of assuring that our units do
//    // what they are supposed to do (testing their interface)
//    // - So, it's a very, very essential skill for you to learn to do efficiently
//
//
//    // Part 15: Tests (ii) Out first unit test
//
//    // - Add a test dependency: [scala-test](https://www.scalatest.org/)
//    // - Write our first unit test
//    // - What is testable code?
//    // - We would like to call a unit which contains a small
//    // self-contained set of functionality
//    // - Therefore, we have to provide the full context of the unit (class, parameters)
//    // - Therefore, we want to keep it as small as possible
//    // - Refactor our code to be easily testable
//
//  }
//}












package org.larinpaul.sparkdev

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit, row_number, year}
import org.apache.spark.sql.types.StringType // SparkSession is part of the sql package...

object Main {
  def main(args: Array[String]): Unit = {

    println("Soon we will test this...")

    val spark = SparkSession.builder()
      .appName("scala-spark")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPI.csv")

    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

    val stockData = df.select(renameColumns: _*)

    import spark.implicits._
    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)
    stockData
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .sort($"close".desc)
      .show()
//      .explain(extended = true)

    println("Soon we will test this...")

  }

}


