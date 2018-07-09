package com.gsvic.spark.cost.model

import com.gsvic.spark.cost.metrics.CostMetrics
import org.apache.spark.sql.execution.SparkPlan

/**
  * The cost model abstraction. Every implementation of each cost model should extend this one.
  * */
trait CostModel {
  val children: List[CostModel]
  val plan: SparkPlan
  val metrics: CostMetrics
  val sizeInBytes: Long
  val rows: Long
  val inPartitions: Long
  val outPartitions: Long
  val getCost: Double

  /**
    * The number of rounds is the degree of parallelism
    * */
  def ROUNDS = Math.ceil(inPartitions.toDouble/metrics.CORES)

  /**
    * Return the total execution cost estimation. This should be the sum of all
    * the previous costs plus the current cost
    * */
  def getTotalCost: Double

  /**
    * Prints the execution cost of each operator in a tree-like format
    * */
  def print(str: String = "|"): Unit = {
    println(str + this + s" -- Current Cost: ${this.getCost}, Total Cost: ${this.getTotalCost}")
    this.children.foreach(c => c.print(str+"-"))
  }

  override def toString: String = {
    s"${this.getClass.getSimpleName}[inPart=$inPartitions, outPart=$outPartitions,rows=${rows}, size=${sizeInBytes}]"
  }
}
