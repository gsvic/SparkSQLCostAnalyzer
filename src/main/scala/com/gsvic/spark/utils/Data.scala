package com.gsvic.spark.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object Data {
  /**
    * Creates a dummy table of n million rows
    * */
  def createNMillionRowsTable(spark: SparkSession, n: Int, partitions: Int, format: String = null, outHDFSPath: String = null,
                              persist: Boolean = false) : DataFrame = {
    val M = 1000000
    val dataRDD = spark.sparkContext.parallelize(1 to n)
      .repartition(partitions)
      .map{ i =>
        val r = ( (i-1)*M+1 to i*M).map(x => (x, s"data${x}"))
        r
      }
      .flatMap(x => x)

    val dataDF = spark.createDataFrame(dataRDD)
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "data")

    if (persist)
      dataDF.write.format(format).save(s"$outHDFSPath/data${n}M${partitions}P.${format}")

    dataDF
  }
}
