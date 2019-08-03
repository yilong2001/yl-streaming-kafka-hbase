package com.example.streaming.pipeline

import java.util.Properties

import org.apache.spark.sql.types.StructType

/**
  * Created by yilong on 2019/4/5.
  */


object TopicDataType extends Enumeration {
  val Avro = Value(100, "avro")
  val Dat = Value(200, "dat")
}

object JoinDataType extends Enumeration {
  val HBaseJoin = Value(200, "hbase_join")
}

object SinkDataType extends Enumeration {
  val None = Value(100, "none")
  val HBaseWrite = Value(200, "hbase_write")
  val KafkaWrite = Value(300, "kafka_write")
  val KafkaCommit = Value(310, "kafka_commit")
}

case class SqlPiplineMetaData(sql:String,
                              tableName: String,
                              sinkTypes:Array[SinkDataType.Value],
                              sinkIds:Array[String],
                              joinTypes:Array[JoinDataType.Value],
                              joinIds:Array[String],
                              watermarkMetaData: WatermarkMetaData)

//for example : fields: col1:int,col2:int,col3:string,...
case class TableSchemaMetaData(tableName: String,
                               cols:String)

case class KafkaCommitMetaData(props : Properties,
                               topic:String)

case class HBaseJoinMetaData(tableName: String,
                             family:String,
                             srcCols:String,
                             destCols:String,
                             rowKeyField:String)

case class HBaseWriteMetaData(tableName: String,
                              family:String,
                              cols:String,
                              opTypeField:String)

case class KafkaMetaData(props : Properties,
                         topic:String,
                         topicType:TopicDataType.Value,
                         schema:String,
                         dataType:StructType,
                         tableName: String)

case class HiveMetaData(tableName:String)


case class TopicPartitionOffsetTimestamp(topic:String,
                                         partition:Int,
                                         offset:Long,
                                         timestamp:Long)

case class WatermarkMetaData(eventTime: String, delayThreshold: String)

case class UDFMetaData(nickName : String,
                       staticFuncName : String)
