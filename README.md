# SparkSQLCostAnalyzer

A tiny framework for Spark SQL cost analysis. This project includes several cost models for Spark SQL, as well as a library for analyzing the cost of a Spark SQL query/DataFrame.

## Example
```
val spark = SparkSession.builder()
    .master("local")
    .appName("CostAnalyzerExample")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()

  spark.conf.set("spark.sql.codegen.wholeStage", false)

  spark.read.parquet("hdfs://localhost:9000/user/gsvictor/data100M36P.parquet").createOrReplaceTempView("a")
  spark.read.parquet("hdfs://localhost:9000/user/gsvictor/data500M20P.parquet").createOrReplaceTempView("b")

  val df = spark.sql("select * from a, b where a.id = b.id and b.id < 10000000")

  val analyzer = new CostAnalyzer(computeIntermediateResults = true)
  analyzer.analyze(df)
```
Output
```
|SortMergeJoinCost[inPart=200, outPart=200,rows=100000, size=17905215] -- Current Cost: 0.004155409022850999, Total Cost: 153.12430200939738
|-SortCost[inPart=200, outPart=200,rows=100000, size=17905215] -- Current Cost: 0.012912183843752454, Total Cost: 40.42083234720785
|--ShuffleCost[inPart=1, outPart=200,rows=100000, size=17905215] -- Current Cost: 39.70299043967368, Total Cost: 40.407920163364096
|---CostModelImpl[inPart=1, outPart=1,rows=100000, size=17905215] -- Current Cost: 0.0, Total Cost: 0.7049297236904205
|----FilterCost[inPart=9, outPart=1,rows=100000, size=17905215] -- Current Cost: 0.7049297236904205, Total Cost: 0.7049297236904205
|-----ScanCost[inPart=9, outPart=9,rows=5000000, size=895269692] -- Current Cost: 7.832630700672134, Total Cost: 7.832630700672134
|-SortCost[inPart=200, outPart=200,rows=99999, size=30045982] -- Current Cost: 0.012912033944868903, Total Cost: 112.69931425316669
|--ShuffleCost[inPart=1, outPart=200,rows=99999, size=30045982] -- Current Cost: 111.50348954326303, Total Cost: 112.68640221922182
|---CostModelImpl[inPart=1, outPart=1,rows=99999, size=30045982] -- Current Cost: 0.0, Total Cost: 1.1829126759587834
|----FilterCost[inPart=40, outPart=1,rows=99999, size=30045982] -- Current Cost: 1.1829126759587834, Total Cost: 1.1829126759587834
|-----ScanCost[inPart=40, outPart=40,rows=14920100, size=4482935372] -- Current Cost: 22.061689741276602, Total Cost: 22.061689741276602
```
