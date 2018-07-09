package org.apache.spark

import com.gsvic.spark.cost.CostAnalyzer
import com.gsvic.spark.utils.Data
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object App extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder()
    .master("local")
    .appName("CostAnalyzerExample")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()

  spark.conf.set("spark.sql.codegen.wholeStage", false)

  val a = Data.createNMillionRowsTable(spark, 5, 10).createOrReplaceTempView("a")
  val b = Data.createNMillionRowsTable(spark, 2, 10).createOrReplaceTempView("b")

  val df = spark.sql("select * from a, b where a.id = b.id and b.id < 100000")

  val analyzer = new CostAnalyzer(computeIntermediateResults = true)
  analyzer.analyze(df)
}
