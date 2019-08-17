package com.example.streaming.demo.framework

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import com.example.streaming.pipeline._
import org.apache.avro.Schema
import org.apache.spark.sql._
import org.apache.spark.sql.types.DataTypes


/**
  * Created by yilong on 2019/3/17.
  */

object StreamingSqlToKafka {
  def buildStartingOffset(topics : String) : String = {
    """{"topic1":{"0":1}}"""
  }

  def udfMetasToString(udfMetas:Array[UDFMetaData]) : String = {
    udfMetas.map(udfmd => {
      "spark.udf.register(\""+udfmd.nickName+"\", "+udfmd.staticFuncName+" _)"
    }).mkString(";")
  }

  def registerUDF(spark: SparkSession, udfMetas:Array[UDFMetaData]) = {
    import com.example.streaming.pipeline.RegMacro._
    val regStr = udfMetasToString(udfMetas)
    println(regStr)
    //RegMacro(spark, regStr)
  }

  def main(args: Array[String]): Unit = {
    val topic1 = "myrecord1"
    val topic2 = "myrecord2"

    val targetTopic = "myrecord5"

    val groupId = "myrecord1.groupid.006"
    val clientid = "myrecord1.clientid."

    val props1 = KafkaDataBuilder.getKafkaProp(groupId, clientid)
    val props2 = KafkaDataBuilder.getKafkaProp(groupId, clientid)
    val props3 = KafkaDataBuilder.getKafkaProp(groupId, clientid)

    val m_schema_str_src_1 = AvroDataBuilder.getComposedSchemaStr()

    val m_schema_str_target = AvroDataBuilder.getRowkeyedSchemaStr()

    val m_schema_str_src_2 = m_schema_str_src_1

    val m_parser = new Schema.Parser();

    val m_scheam_1 = m_parser.parse(m_schema_str_src_1)
    props1.put("client.id", clientid+"001")
    val kafkaSrc1MetaData = KafkaMetaData(props1, topic1,
      TopicDataType.Avro,
      m_schema_str_src_1,
      AvroSchemaPlainConverter.convert(m_scheam_1),
      "table1")

    val m_scheam_2 = m_scheam_1;
    props2.put("client.id", clientid+"002")
    val kafkaSrc2MetaData = KafkaMetaData(props2, topic2,
      TopicDataType.Avro,
      m_schema_str_src_2,
      AvroSchemaPlainConverter.convert(m_scheam_2),
      "table2")

    val m_scheam_3 = m_parser.parse(m_schema_str_target)
    val kafkaTargetMetaData = KafkaMetaData(props2, targetTopic,
      TopicDataType.Avro,
      m_schema_str_target,
      AvroSchemaPlainConverter.convert(m_scheam_3),
      "target_t1")

    val tableSchemaList = Array(
      TableSchemaMetaData("myrecord_t1", "uid:string,nick:string,grade:int,timestamp:long")
    )

    val myrecordT1HBaseMetaData = HBaseWriteMetaData("myrecord_t1", "f", "uid,uid_j1,nick,grade,grade_j1,timestamp","op_type")
    val myrecordT1KafkaCommitMetaData = KafkaCommitMetaData(props3, "commit_t1")
    val myrecordT1HBaseJoinMetaData = HBaseJoinMetaData("myrecord_t1","f","uid,grade","uid_j1,grade_j1","rowkey")

    val sinkMap = new java.util.HashMap[String,Any]()
    sinkMap.put("target_t1", kafkaTargetMetaData)
    sinkMap.put("hbase_iud_1_hbase", myrecordT1HBaseMetaData)
    sinkMap.put("hbase_iud_1_kafkacommit", myrecordT1KafkaCommitMetaData)
    sinkMap.put("hbase_iud_1_hbase_join", myrecordT1HBaseJoinMetaData)

    val kafkaSrcList = Array(kafkaSrc1MetaData, kafkaSrc2MetaData)

    val sqlPipList = Array(
      SqlPiplineMetaData("select * from global_temp.table1 ", "tmp_table1",
        Array(SinkDataType.None), Array[String]("tmp_table1"),
        null,null,null)
      /* SqlPiplineMetaData("select th1.`topic.timestamp`, t1.`before.uid` as uid_b1, t1.`before.nick` as nick_b1, " +
        "t1.`before.grade` as grade_b1, t2.`after.uid` as uid_a2, t2.`after.nick` as nick_a2, " +
        "t2.`after.grade` as grade_a2 from global_temp.table1 as t1 left join global_temp.table2 " +
        "as t2 where t1.`before.uid`=t2.`after.uid`","tmp_table1",
        Array(SinkDataType.None), Array[String]("tmp_table1"),
        null,null,WatermarkMetaData("topic.timestamp", "10 seconds")) */

      // spark streaming 生成的临时内存表 global_temp.tmp_table1 与 hive 中的表 demo_parquet1
      // 做 left join, 如果是 inner join 可能丢失待 committed kafka offset
      /*SqlPiplineMetaData("select t1.*, t2.name as t2_name from global_temp.tmp_table1 t1 " +
        "left join demo_parquet1 t2 on t1.uid_b1 = t2.name ","tmp_hive_join_table1",
        Array(SinkDataType.None), Array[String]("tmp_hive_join_table1"),
        null,null,null),

      SqlPiplineMetaData("select th1.`after.uid` as uid, th1.`after.nick` as nick, th1.`after.grade` as grade" +
        ",case when th1.op_type='I' then concat(th1.`after.uid`,'_',th1.`after.nick`) " +
        "      when th1.op_type='D' then concat(th1.`before.uid`,'_',th1.`before.nick`) " +
        "      when th1.op_type='U' then concat(th1.`after.uid`,'_',th1.`after.nick`) end as rowkey," +
        "'default' as joinkey1, 1 as const_1, th1.`topic.partition` as t_par, " +
        "th1.`topic.offset` as t_off, th1.`topic.timestamp` as timestamp, th1.op_type from global_temp.table1 th1 ",
        "hbase_iud_1",
        Array(SinkDataType.HBaseWrite, SinkDataType.KafkaCommit),
        Array("hbase_iud_1_hbase", "hbase_iud_1_kafkacommit"),
        Array(JoinDataType.HBaseJoin),
        Array("hbase_iud_1_hbase_join"),null),

      SqlPiplineMetaData("select uid_b1,nick_b1,uid_a2,nick_a2 from global_temp.tmp_table1","tmp_table2",
        Array(SinkDataType.None),
        Array("tmp_table2"),
        null,null,null),

      SqlPiplineMetaData("select uid_b1 as uid,nick_a2 as nick, grade_a2 as grade, concat(uid_b1,'+',nick_a2) as rowkey from global_temp.tmp_hive_join_table1","target_t1",
        Array(SinkDataType.KafkaWrite),
        Array("target_t1"),
        null,null,null)
        */
    )

    val udfMetas = Array{UDFMetaData("isAbnormalEx1", "com.example.streaming.myudf.IsAbnormal.isAbnormalEx")}

    val spark = SparkSession.builder
      .appName("KafkaStreamingDemo1")
      .master("spark://localhost:7077")
      .config("spark.streaming.kafka.consumer.cache.enabled",false)
      .config("spark.local.dir","~/setup/tmp/spark2.4/tmp")
      .config("spark.work.dir", "~/setup/tmp/spark2.4/work")
      //hive 中的表，可以直接用
      //.config("hive.metastore.uris", "thrift://localhost:9083")
      //.enableHiveSupport()
      .getOrCreate()

    registerUDF(spark, udfMetas)

    PipeEngine(spark, kafkaSrcList, sinkMap, sqlPipList, tableSchemaList).start()
  }
}
