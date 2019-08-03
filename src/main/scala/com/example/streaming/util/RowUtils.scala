package com.example.streaming.util

import com.example.streaming.pipeline.TopicPartitionOffsetTimestamp
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.StructType

/**
  * Created by yilong on 2019/4/6.
  */
object RowUtils {
  def getTopicPartitionOffsetTimestampFromRow(in: org.apache.spark.sql.Row): TopicPartitionOffsetTimestamp = {
    val topic = in.getAs[String]("topic")
    val partition = in.getAs[Int]("partition")
    val offset = in.getAs[Long]("offset")
    val timestamp = in.getAs[java.sql.Timestamp]("timestamp").getTime

    val tpot = TopicPartitionOffsetTimestamp(topic, partition, offset, timestamp)

    tpot
  }

  def appendTopicValuesToRow(row: org.apache.spark.sql.Row,
                             tpot:TopicPartitionOffsetTimestamp,
                             newSt: StructType): org.apache.spark.sql.Row = {
    val outObjArr = row.toSeq.toArray ++ Array(
      tpot.topic, tpot.partition, tpot.offset, tpot.timestamp
    )

    new GenericRowWithSchema(outObjArr, newSt)
  }
}
