package com.gsvic.spark.cost

import com.gsvic.spark.cost.metrics.CostMetrics
import com.gsvic.spark.cost.model._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution._

class CostAnalyzer(val computeIntermediateResults: Boolean = false) {
  val metrics = new CostMetrics

  def analyze(df: DataFrame): Unit = {
    getOperatorCost(df.queryExecution.executedPlan)
  }

  def getOperatorCost(sparkPlan: SparkPlan, str: String="|"): Unit ={

    val cost = matchCostModel(sparkPlan)
    cost.print()
  }

  private def matchCostModel(sparkPlan: SparkPlan): CostModel ={
    sparkPlan match {
      case x: FileSourceScanExec => {
        new ScanCost(List.empty, sparkPlan, x.relation.sizeInBytes, x.execute().count(), x.execute().partitions.size, metrics)
      }
      case filterExec: FilterExec => {
        val children = List(matchCostModel(sparkPlan.children(0)))
        new FilterCost(children, filterExec, metrics, computeIntermediateResults)
      }
      case exchangeExec: ShuffleExchangeExec => {
        val children = sparkPlan.children.map(matchCostModel).toList
        new ShuffleCost(children, exchangeExec, metrics)
      }
      case sortExec: SortExec => {
        val children = sparkPlan.children.map(matchCostModel).toList
        new SortCost(children, sortExec, metrics)
      }
      case sortMergeJoinExec: SortMergeJoinExec => {
        val children = sparkPlan.children.map(matchCostModel).toList
        new SortMergeJoinCost(children, sortMergeJoinExec, metrics)
      }
      case _ => {
        val children = sparkPlan.children.map(matchCostModel).toList
        new CostModelImpl(children, sparkPlan, metrics)
      }
    }
  }

}
