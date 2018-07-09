package com.gsvic.spark.cost.model
import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.SparkPlan

/**
  * The cost model of [[org.apache.spark.sql.execution.FileSourceScanExec]] operator
  * */
class ScanCost(override val children: List[CostModel] , override val plan: SparkPlan,
               override val sizeInBytes: Long, override val rows: Long,  override val inPartitions: Long,
               override val metrics: CostMetrics) extends CostModel {

  val partitionSize = sizeInBytes/inPartitions
  val diskReadRate = metrics.DISK_READ
  val networkReadRate = metrics.NET

  override val outPartitions: Long = inPartitions
  override val getCost: Double = {
    partitionSize * (diskReadRate + networkReadRate) * ROUNDS
  }

  override def getTotalCost: Double = this.getCost
}
