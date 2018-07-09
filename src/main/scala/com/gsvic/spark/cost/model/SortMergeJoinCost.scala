package com.gsvic.spark.cost.model

import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

/**
  * Cost model implementation of [[SortMergeJoinExec]] operator
  * */
class SortMergeJoinCost(override val children: List[CostModel], override val plan: SortMergeJoinExec,
                        override val metrics: CostMetrics) extends CostModelImpl(children, plan, metrics) {

  override val getCost: Double = {
    val leftRows = children(0).rows
    val rightRows = children(1).rows
    val rowsPerPartition = (leftRows + rightRows) / inPartitions.toDouble
    val cost = rowsPerPartition * metrics.CPU * ROUNDS

    cost
  }

}
