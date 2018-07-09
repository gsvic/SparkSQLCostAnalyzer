package com.gsvic.spark.cost.model

import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

/**
  * Cost model implementation of [[BroadcastHashJoinExec]] operator
  * */
class BroadcastHashJoinCost(override val children: List[CostModel], override val plan: BroadcastHashJoinExec,
                            override val metrics: CostMetrics) extends CostModelImpl(children, plan, metrics) {

  val bigRelation = if (children(0).sizeInBytes > children(1).sizeInBytes) children(0) else children(1)
  val smallRelation = if (children(0).sizeInBytes > children(1).sizeInBytes) children(1) else children(0)

  override val inPartitions: Long = bigRelation.outPartitions
  override val outPartitions: Long = bigRelation.outPartitions

  override val getCost: Double = {

    val hashCost = smallRelation.rows * metrics.HASH
    val broadcastCost = smallRelation.sizeInBytes * metrics.NET

    val innerLoopJoin = ((smallRelation.rows * bigRelation.rows * metrics.CPU) / bigRelation.outPartitions)*ROUNDS

    hashCost + broadcastCost + innerLoopJoin
  }

}
