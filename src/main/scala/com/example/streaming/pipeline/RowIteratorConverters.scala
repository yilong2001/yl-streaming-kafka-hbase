package com.example.streaming.pipeline

import java.util

import com.example.streaming.util.{BytesUtils, RowUtils}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by yilong on 2019/4/5.
  */

case class HBaseJoinRowConverter(tableColMap:java.util.Map[String,java.util.Map[String,String]],
                              newSt: StructType) {
  private val log = LogFactory.getLog(classOf[HBaseJoinRowConverter])

  var hbaseconf : org.apache.hadoop.conf.Configuration = null
  var conn : org.apache.hadoop.hbase.client.Connection = null
  val tableMDMap = new java.util.HashMap[String, HBaseJoinMetaData]()

  def add(metaData : HBaseJoinMetaData) : Unit = {
    tableMDMap.put(metaData.tableName, metaData)
  }

  def connect() : Unit = {
    try {
      hbaseconf = HBaseConfiguration.create()

      hbaseconf.set("hbase.client.pause", "50");
      hbaseconf.set("hbase.client.retries.number", "3");
      hbaseconf.set("hbase.rpc.timeout", "2000");
      hbaseconf.set("hbase.client.operation.timeout", "3000");
      hbaseconf.set("hbase.client.scanner.timeout.period", "10000");

      conn = ConnectionFactory.createConnection(hbaseconf)
    } catch {
      case e : Exception => log.error(e); throw e
    }
  }

  def convert(in : org.apache.spark.sql.Row, outArray : Array[Any]): util.Map[Int,Any] = {
    val outObjMap = new util.HashMap[Int,Any]()

    tableMDMap.foreach(tp => {
      try {
        if (conn == null || conn.isClosed || conn.isAborted) {
          conn = ConnectionFactory.createConnection(hbaseconf)
        }

        val metaData = tp._2
        val table = conn.getTable(TableName.valueOf(metaData.tableName))
        val rowkey = BytesUtils.sparkRowToHBaseBytes(in.get(in.fieldIndex(metaData.rowKeyField)))
        val result = table.get(new Get(rowkey))
        val colMap = tableColMap.get(tp._1)

        for ((dcol, scol) <- colMap) {
          val index = newSt.fieldIndex(dcol)
          val dt = newSt.fields.apply(index).dataType
          val byteV = result.getValue(BytesUtils.sparkRowToHBaseBytes(metaData.family),
            BytesUtils.sparkRowToHBaseBytes(scol))

          //outArray(index) =
          outObjMap.put(index, BytesUtils.arrayByteToObj(byteV, dt))
        }

        table.close()
      } catch {
        case e : Exception => {
          log.error(e)
        }
      }
    })

    outObjMap
  }

  def close() : Unit = {
    try {
      conn.close()
    } catch {
      case e : Exception => log.error(e)
    }
  }
}

object RowIteratorConverters {
  private val log = LogFactory.getLog(classOf[PipeEngine])

  /*
  *
  * */
  def avroRecordIteratorConverter(kmd: KafkaMetaData)(iterator: Iterator[org.apache.spark.sql.Row]): Iterator[org.apache.spark.sql.Row] = {
    val schema = new Schema.Parser().parse(kmd.schema)
    iterator.map(in => {
      val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(in.getAs[Array[Byte]]("value"), null)
      val data = reader.read(null, decoder)

      val tpot = RowUtils.getTopicPartitionOffsetTimestampFromRow(in)

      val out = EnDecoder.encodeAvroRecordIntoRow(data, kmd.dataType, tpot)

      out
    })
  }

  def datRecordIteratorConverter(kmd: KafkaMetaData)(iterator: Iterator[org.apache.spark.sql.Row]): Iterator[org.apache.spark.sql.Row] = {
    iterator.map(in => {
      val out = in

      out
    })
  }


  /*
  * joinMDs : all HBaseJoinMetaDatas used to sql pipe line
  * colMap : (tableName vs (destCol vs srcCol))
  * newStructType : New StructType after adding new join fields
  * */
  def JoinIteratorConverter(joinMDs:Array[Any],
                            tableColMap:java.util.Map[String,java.util.Map[String,String]],
                            newStructType: StructType)(iterator: Iterator[org.apache.spark.sql.Row]): Iterator[org.apache.spark.sql.Row] = {
    val hBaseJoinRowConverter = HBaseJoinRowConverter(tableColMap, newStructType)
    //TODO: here, add other join converter

    joinMDs.foreach(md => {
      md match {
        case HBaseJoinMetaData(_,_,_,_,_) => hBaseJoinRowConverter.add(md.asInstanceOf[HBaseJoinMetaData])
        case _ => None
      }
    })

    try {
      hBaseJoinRowConverter.connect()
      //TODO: here, other join convert init
    } catch {
      case e : Exception => hBaseJoinRowConverter.close(); log.error(e); throw e;
    }

    iterator.map(in => {
      val objectArray = new Array[Any](newStructType.fields.size)
      val hbaseJoinOutMap = hBaseJoinRowConverter.convert(in, objectArray)
      hbaseJoinOutMap.foreach(tp => objectArray(tp._1)=tp._2)
      //TODO: here, other converter

      //最后，复制 in row fields ，生成新的 out Row
      in.schema.fields.foreach(sf => {
        val index = newStructType.fieldIndex(sf.name)
        objectArray(index) = in.get(in.fieldIndex(sf.name))
      })

      new GenericRowWithSchema(objectArray, newStructType)
    })
  }
}
