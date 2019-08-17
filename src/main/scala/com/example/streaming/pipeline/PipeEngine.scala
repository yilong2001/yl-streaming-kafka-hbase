package com.example.streaming.pipeline

import com.example.streaming.util.MetaUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._
import scala.reflect.ClassTag


/**
  * Created by yilong on 2019/4/6.
  */
case class PipeEngine(spark : SparkSession,
                      kmdSrcArray : Array[KafkaMetaData],
                      sinkMap : java.util.Map[String,Any],
                      sqlPipArray : Array[SqlPiplineMetaData],
                      tableSchemas:Array[TableSchemaMetaData]) {
  final val Default_Porcessing_Period = 5000

  private val log = LogFactory.getLog(classOf[PipeEngine])
  //all broadcast var
  val m_kafka_src_array_bc =  spark.sparkContext.broadcast[Array[KafkaMetaData]](kmdSrcArray)
  val m_sink_meta_map_bc = spark.sparkContext.broadcast[java.util.Map[String,Any]](sinkMap)
  val m_tables_schema_bc = spark.sparkContext.broadcast[Array[TableSchemaMetaData]](tableSchemas)

  private def createSStream(sparkSession: SparkSession, kmd: KafkaMetaData) : org.apache.spark.sql.DataFrame = {
    val ss = sparkSession.readStream.format("kafka").options(kmd.props)
      .option("subscribe", kmd.topic)
      .option("startingOffsets", "earliest")
        .option("group.id", "group.id.12345")
      //.option("startingOffsets", buildStartingOffset(topics))
      .load()

    log.info("___________________ stream source schema _________________________")
    ss.printSchema()

    ss
  }

  private def createKafkaStreams() :  Array[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = {
    kmdSrcArray.zipWithIndex.map(tuple => {
      val encoder = RowEncoder(MetaUtils.appendTopicFieldsToStructType(tuple._1.dataType.asInstanceOf[StructType]))
      val kmd = m_kafka_src_array_bc.value.apply(tuple._2)
      val ss = tuple._1.topicType match {
        case TopicDataType.Avro => {
          createSStream(spark,tuple._1).mapPartitions(RowIteratorConverters.avroRecordIteratorConverter(kmd))(encoder)
        }
        case TopicDataType.Dat => {
          createSStream(spark,tuple._1).mapPartitions(RowIteratorConverters.datRecordIteratorConverter(kmd))(encoder)
        }
        case _ => {//TODO:default is Dat format
          createSStream(spark,tuple._1).mapPartitions(RowIteratorConverters.datRecordIteratorConverter(kmd))(encoder)
        }
      }

      ss.createOrReplaceGlobalTempView(tuple._1.tableName)

      ss
    })
  }

  private def doPipeJoinOperation(pipeMetaData : SqlPiplineMetaData,
                                  inDF : org.apache.spark.sql.DataFrame) :
  org.apache.spark.sql.DataFrame = {
    val outDF = if (pipeMetaData.joinTypes == null || pipeMetaData.joinTypes.size == 0) {
      inDF
    } else {
      val tableSchemas = m_tables_schema_bc.value
      val tableColsMap = new java.util.HashMap[String,java.util.Map[String,String]]()
      val allJoinMetaDatas = pipeMetaData.joinIds.map(id=>{
        m_sink_meta_map_bc.value.get(id)
      })

      val joinDatas = pipeMetaData.joinTypes.zip(pipeMetaData.joinIds).map(tp => {
        val sinkMetaData = m_sink_meta_map_bc.value.get(tp._2)
        tp._1 match {
          case JoinDataType.HBaseJoin => {
            val joinMetaData = sinkMetaData.asInstanceOf[HBaseJoinMetaData]
            val fieldTypes = MetaUtils.parseTableSchemaString(MetaUtils.getTableSchemaMetaData(joinMetaData.tableName, tableSchemas))

            val cols = joinMetaData.destCols.split(MetaUtils.DEFAULT_FIELD_SPLIT_CHAR)
              .zip(joinMetaData.srcCols.split(MetaUtils.DEFAULT_FIELD_SPLIT_CHAR))

            val joinCols = cols.map(tp => {
              tp._1+MetaUtils.DEFAULT_FIELD_TYPE_SPLIT_CHAR+fieldTypes.get(tp._2)
            })

            val colMap = new java.util.HashMap[String,String]()
            cols.foreach(tp => colMap.put(tp._1, tp._2))

            tableColsMap.put(joinMetaData.tableName, colMap)
            joinCols.mkString(MetaUtils.DEFAULT_FIELD_SPLIT_CHAR)
          }
          case _ => {
            null
          }
        }
      }).filter(_ != null)

      val allJoinCols = joinDatas.mkString(MetaUtils.DEFAULT_FIELD_SPLIT_CHAR)
      val newStructType = MetaUtils.appendJoinFieldsToStructType(inDF.schema, allJoinCols)
      val encoder = RowEncoder(newStructType)

      inDF.mapPartitions(RowIteratorConverters.JoinIteratorConverter(allJoinMetaDatas,tableColsMap, newStructType))(encoder)
    }

    outDF
  }

  private def doPipeSinkOperation(pipeMetaData : SqlPiplineMetaData,
                                  inDF : org.apache.spark.sql.DataFrame) : org.apache.spark.sql.streaming.StreamingQuery = {
    val qout = if (pipeMetaData.sinkTypes == null || pipeMetaData.sinkTypes.length < 1) {
      inDF.writeStream.trigger(ProcessingTime(Default_Porcessing_Period)).outputMode("append").format("console").start()
    } else if (pipeMetaData.sinkTypes.length == 1 && pipeMetaData.sinkTypes(0) == SinkDataType.None) {
      inDF.writeStream.trigger(ProcessingTime(Default_Porcessing_Period)).outputMode("append").format("console").start()
    } else {
      val pipes1 = pipeMetaData.sinkTypes.zipWithIndex.map(tp => {
        val sinkMetaData = m_sink_meta_map_bc.value.get(pipeMetaData.sinkIds(tp._2))
        tp._1 match {
          case SinkDataType.None => new DefaultForeachWriter()
          case SinkDataType.HBaseWrite => {
            new HBaseWriteForeachWriter(sinkMetaData.asInstanceOf[HBaseWriteMetaData])
          }
          case SinkDataType.KafkaCommit => {
            new KafkaCommiterForeachWriter(sinkMetaData.asInstanceOf[KafkaCommitMetaData])
          }
          case SinkDataType.KafkaWrite => {
            val md = sinkMetaData.asInstanceOf[KafkaMetaData]
            new KafkaWriteForeachWriter(md, md.dataType.fields.map(sf=>sf.name))
          }
          case _ => {
            throw new IllegalArgumentException("SinkType is unsupported : " + tp._1.toString)
          }
        }
      })

      log.info("___________________ composed foreach writers ________________________________")
      pipes1.foreach(log.info)

      inDF.writeStream
        .foreach(new ComposedForeachWriter(pipes1))
        .trigger(ProcessingTime(Default_Porcessing_Period)).outputMode("append").start()
    }

    qout
  }

  def start(): Unit = {
    val srcDFs = createKafkaStreams()

    log.info("___________________ kafka src df schema _________________________")
    srcDFs.foreach(_.printSchema())

    srcDFs.foreach(ds => ds.writeStream.trigger(ProcessingTime(Default_Porcessing_Period)).outputMode("append").format("console").start())

    val streamingQuerys = sqlPipArray.zipWithIndex.map(tuple => {
      val q0 = spark.sql(tuple._1.sql)
      log.info("___________________ stream query df schema : begin _________________________")
      log.info(tuple._1)
      tuple._1.sinkIds.zip(tuple._1.sinkTypes).foreach(log.info)

      val q1 = doPipeJoinOperation(tuple._1, q0)

      q1.printSchema()
      log.info("___________________ stream query df schema : end _________________________")

      if (tuple._1.tableName != null && tuple._1.tableName.length > 0) {
        q1.createOrReplaceGlobalTempView(tuple._1.tableName)
      }

      val q2 = (tuple._1.watermarkMetaData != null) match {
        case true => {
          q1.withWatermark(tuple._1.watermarkMetaData.eventTime, tuple._1.watermarkMetaData.delayThreshold)
        }
        case _ => q1
      }

      doPipeSinkOperation(tuple._1, q2)
    })

    log.info("___________________ start stream waiting ... _________________________")
    streamingQuerys.foreach(sq => sq.explain(true))

    import org.apache.spark.sql.streaming.StreamingQueryException
    try
      spark.streams.awaitAnyTermination()
    catch {
      case e: StreamingQueryException =>
        e.printStackTrace()
    }
  }
}
