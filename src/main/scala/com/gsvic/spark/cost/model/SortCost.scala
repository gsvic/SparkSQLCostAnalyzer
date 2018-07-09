package com.gsvic.spark.cost.model

import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.SortExec

/**
  * The implementation of [[SortExec]] cost model. We assume that Tim Sort is being used
  * */
class SortCost(override val children: List[CostModel], override val plan: SortExec,
               override val metrics: CostMetrics) extends CostModelImpl(children, plan, metrics) {

  override val getCost: Double = {
    val inRows = children(0).rows / outPartitions.toDouble
    val cpuCost = inRows * math.log(inRows) * metrics.CPU
    val cost = cpuCost * ROUNDS

    cost
  }
}
