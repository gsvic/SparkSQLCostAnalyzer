package com.gsvic.spark.cost.metrics

import java.io.FileInputStream
import java.util.Properties

class CostMetrics {
  val props = new Properties
  val confFileStream = new FileInputStream("conf/cost.properties")
  props.load(confFileStream)

  final val DISK_READ: Double =  props.getProperty("disk.read").toDouble
  final val DISK_WRITE: Double = props.getProperty("disk.write").toDouble
  final val NET: Double = props.getProperty("network").toDouble
  final val CORES: Double = props.getProperty("cores").toDouble
  final val CPU: Double = props.getProperty("cpu").toDouble
  final val HASH: Double = props.getProperty("hash").toDouble
  final val DEFAULT_SHUFFLE_PARTITIONS: Long = props.getProperty("shuffle.partitions").toLong
}
