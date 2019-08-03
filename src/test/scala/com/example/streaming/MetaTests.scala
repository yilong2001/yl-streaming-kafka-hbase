package com.example.streaming

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import com.example.streaming.pipeline._
import org.apache.avro.Schema

/**
  * Created by yilong on 2019/4/16.
  */
object MetaTests {
  def main(args: Array[String]): Unit = {
    val topic1 = "myrecord1"
    val topic2 = "myrecord2"

    val targetTopic = "myrecord4"

    val groupId = "myrecord1.groupid.006"
    val clientid = "myrecord1.clientid.012"

    val props = KafkaDataBuilder.getKafkaProp(groupId, clientid)

    val m_schema_str_src_1 = AvroDataBuilder.getComposedSchemaStr()

    val m_schema_str_target = AvroDataBuilder.getRowkeyedSchemaStr()

    val m_schema_str_src_2 = m_schema_str_src_1

    val m_parser = new Schema.Parser();

    val m_scheam_1 = m_parser.parse(m_schema_str_src_1)
    val m_scheam_2 = m_scheam_1;
    val m_scheam_3 = m_parser.parse(m_schema_str_target)

    val kmdTarget = KafkaMetaData(props, targetTopic,
      TopicDataType.Avro,
      m_schema_str_target,
      AvroSchemaPlainConverter.convert(m_scheam_3),
      "target_t1")

    val hmdMyrecordT1 = HBaseWriteMetaData("myrecord_t1", "f", "uid,uid_j1,nick,grade,grade_j1,timestamp","op_type")
    val kcommitMyrecordT1 = KafkaCommitMetaData(props, "commit_t1")

    val hjmdT1 = HBaseJoinMetaData("myrecord_t1","f","uid,grade","uid_j1,grade_j1","rowkey")

    val t = SqlPiplineMetaData("select th1.`after.uid` as uid, th1.`after.nick` as nick, th1.`after.grade` as grade" +
      ",case when th1.op_type='I' then concat(th1.`after.uid`,'_',th1.`after.nick`) " +
      "      when th1.op_type='D' then concat(th1.`before.uid`,'_',th1.`before.nick`) " +
      "      when th1.op_type='U' then concat(th1.`after.uid`,'_',th1.`after.nick`) end as rowkey," +
      "1 as const_1, th1.`topic.partition` as t_par, " +
      "th1.`topic.offset` as t_off, th1.`topic.timestamp` as timestamp, th1.op_type from global_temp.table1 th1 ",
      "hbase_iud_1",
      Array(SinkDataType.HBaseWrite, SinkDataType.KafkaCommit),
      Array("hbase_iud_1_hbase","hbase_iud_1_kafkacommit"),null,null,null).sinkTypes.zipWithIndex.map(tp => {
      tp._1 match {
        case SinkDataType.None => new DefaultForeachWriter()
        case SinkDataType.HBaseWrite => {
          new HBaseWriteForeachWriter(hmdMyrecordT1)
        }
        case SinkDataType.KafkaCommit => {
          new KafkaCommiterForeachWriter(kcommitMyrecordT1)
        }
        case SinkDataType.KafkaWrite => {
          new KafkaWriteForeachWriter(kmdTarget,Array())
        }
        case _ => {
          throw new IllegalArgumentException("SinkType is unsupported : " + tp._1.toString)
        }
      }
    })

    for (tt <- t) {
      val op = tt.open(0,0)
      println("******************:"+op)
      assert(op)
    }
  }
}
