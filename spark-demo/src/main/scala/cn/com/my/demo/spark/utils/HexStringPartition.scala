package cn.com.my.demo.spark.utils

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.spark.Partitioner

class HexStringPartition (partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  //计算region的split键值，总数为partitions-1个
  val splits = new HexStringSplit().split(partitions).map(s => Bytes.toString(s))

  //根据rowkey前缀，计算该条记录输入哪一个region范围内
  def getPartitionNum(splits: Array[String], key: Any): Int = {
    var i = 0
    var foundIt = false
    while (i < splits.length && !foundIt) {
//      if (key.asInstanceOf[(String, String)]._1.substring(0, 8) < splits(i)) {
//        foundIt = true
//      }
      if (key.toString.substring(0, 8) < splits(i)) {
        foundIt = true
      }
      i = i + 1
    }
    i
  }

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => getPartitionNum(splits, key)
  }
}
