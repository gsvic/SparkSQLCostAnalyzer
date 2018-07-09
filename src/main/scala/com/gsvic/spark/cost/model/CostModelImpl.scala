package com.gsvic.spark.cost.model
import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.SparkPlan

/**
  * A generic cost model. For any operator for which no cost model is implemented we should use this cost model.
  * This generic cost model return a zero cost
  * */
class CostModelImpl(override val children: List[CostModel], override val plan: SparkPlan, val metrics: CostMetrics)
  extends CostModel {
  override val sizeInBytes: Long = if (children.size > 0) children(0).sizeInBytes else 0
  override val rows: Long = if (children.size > 0) children(0).rows else plan.execute().count()
  override val inPartitions: Long = if (children.size > 0) children(0).outPartitions else plan.execute().partitions.size
  override val outPartitions: Long = inPartitions
  override val getCost: Double = 0.0

  override def getTotalCost: Double = {
    val totalCost = children.map(_.getTotalCost).sum
    totalCost + this.getCost
  }

}
