package com.gsvic.spark.cost.model

import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}

/**
  * The cost model of [[FilterExec]] operator
  * If the [[computeIntermediateResults]] parameter is set to true, then the actual query is executed to get
  * the real number of rows. Else, an explicit selectivity of 1/3 is used.
  * */
class FilterCost(override val children: List[CostModel], override val plan: FilterExec,
                 override val metrics: CostMetrics, val computeIntermediateResults: Boolean)
  extends CostModelImpl(children, plan, metrics) {

  val selectivity: Double = {
    if (computeIntermediateResults) {
      val currentRows = plan.execute().count
      (currentRows.toDouble / children(0).rows)
    }
    else {
      1/3.0
    }
  }

  // Partition pruning based on selectivity
  override val outPartitions: Long = math.ceil(children(0).outPartitions * selectivity).toLong
  // Byte size reduction based on selectivity
  override val sizeInBytes: Long = math.ceil(children(0).sizeInBytes * selectivity).toLong
  // Row selectivity
  override val rows: Long = math.ceil(children(0).rows * selectivity).toLong


  /* Filter has instant impact to the scan cost, thus, we overriding it */
  val cost = new ScanCost(children = children, plan = plan, sizeInBytes = sizeInBytes, rows = rows,
    inPartitions = outPartitions, metrics = metrics)

  override val getCost: Double = cost.getCost
  override def getTotalCost: Double = getCost

}
