package com.gsvic.spark.cost.model

import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
  * This class implements the cost model of [[ShuffleExchangeExec]] operator. Currently
  * only the shuffle read and write costs are taken into account.
  * */
class ShuffleCost(override val children: List[CostModel], override val plan: ShuffleExchangeExec,
                  override val metrics: CostMetrics) extends CostModelImpl(children, plan, metrics) {

  override val outPartitions: Long = plan.conf.getAllConfs.get("spark.sql.shuffle.partitions")
    .getOrElse(metrics.DEFAULT_SHUFFLE_PARTITIONS).asInstanceOf[Long]

  override val getCost: Double = {
    // Shuffle read
    val inPartitionSize = children(0).sizeInBytes / inPartitions.toDouble
    val inRowsPerPartition = children(0).rows / inPartitionSize.toDouble
    val diskWriteCost = metrics.DISK_WRITE * inPartitionSize

    // Shuffle read
    val outPartitionSize =children(0).sizeInBytes / outPartitions.toDouble
    val diskReadCost = metrics.DISK_READ * outPartitionSize

    val writeCost = diskWriteCost * ROUNDS
    val readCost = diskReadCost * outPartitionSize / metrics.CORES.toDouble

    val cost = writeCost + readCost

    cost
  }

}
