package com.example.streaming.pipeline

import java.util

import com.example.streaming.util.BytesUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{ForeachWriter, Row}

import scala.collection.JavaConversions._

/**
  * Created by yilong on 2019/3/28.
  */
class DefaultForeachWriter() extends ForeachWriter[Row] {
  private val log = LogFactory.getLog(classOf[DefaultForeachWriter])

  override def open(partitionId: Long, epochId: Long): Boolean = {
    true
  }

  override def process(value: Row): Unit = {

  }

  override def close(errorOrNull: Throwable): Unit = {
  }
}

class KafkaWriteForeachWriter(kmd : KafkaMetaData, destCols : Array[String]) extends ForeachWriter[Row] {
  private val log = LogFactory.getLog(classOf[KafkaWriteForeachWriter])

  var producer : KafkaProducer[String, Array[Byte]] = null
  var schema : Schema =  null
  override def open(partitionId: Long, epochId: Long): Boolean = {
    producer = new KafkaProducer[String, Array[Byte]](kmd.props)
    schema =  new Schema.Parser().parse(kmd.schema)
    true
  }
  override def process(row: Row): Unit = {
    val avroWriter = new SpecificDatumWriter[GenericRecord](schema)
    val avro = EnDecoder.encodeRowIntoAvroRecord(schema, destCols, row)
    val rd = EnDecoder.encodeAvroRecord(avroWriter, avro)
    sendKafkaRecord(kmd, producer, rd)
  }
  override def close(errorOrNull: Throwable): Unit = {
    producer.close()
    if (errorOrNull != null) log.error(errorOrNull)
  }

  private def sendKafkaRecord(kmd : KafkaMetaData,
                      producer : KafkaProducer[String, Array[Byte]],
                      record : Array[Byte]) : Unit = {
    val result = producer.send(new ProducerRecord[String, Array[Byte]](kmd.topic, record))
    log.info("*************** procuder send offset : " + kmd.topic + " : " + result.get().offset())
  }
}

class KafkaCommiterForeachWriter(kmmd:KafkaCommitMetaData) extends ForeachWriter[Row] {
  private val log = LogFactory.getLog(classOf[KafkaCommiterForeachWriter])

  val partitions = new util.ArrayList[Long]()
  override def process(row: Row): Unit = {
    val prt = row.getAs[Int]("t_par")
    val off = row.getAs[Long]("t_off")
    log.info(prt+"______:::______:::________"+off)
    partitions.add(off)
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull!=null) log.error(errorOrNull)
    var strout = ""
    partitions.foreach(off => {strout+=(off+"-+-+-+-+-")})
    log.info(strout)
  }
  override def open(partitionId: Long, epochId: Long): Boolean = {
    partitions.clear()
    true
  }
}

class HBaseWriteForeachWriter(hmd:HBaseWriteMetaData) extends ForeachWriter[Row] {
  private val log = LogFactory.getLog(classOf[HBaseWriteForeachWriter])

  var hbaseconf : org.apache.hadoop.conf.Configuration = null
  var tableName : TableName = null
  var table : org.apache.hadoop.hbase.client.Table = null
  var conn : org.apache.hadoop.hbase.client.Connection = null

  //TODO: when conn exception, conn should reconnect!
  override def process(row: Row): Unit = {
    if (conn == null || table == null) {
      throw new IllegalArgumentException("hbase table="+tableName+" , conn or table is null!")
    }
    try {
      val optype = row.getAs[String](hmd.opTypeField)
      val rowkey = BytesUtils.sparkRowToHBaseBytes(row.get(row.fieldIndex("rowkey")))
      log.info(optype+"______:::______:::________"+row.get(row.fieldIndex("rowkey")))

      if (optype.equals("I")) {
        val put = new org.apache.hadoop.hbase.client.Put(rowkey)
        hmd.cols.split(",").filter(!_.equalsIgnoreCase("rowkey")).foreach(col => {
          var fi: Int = -1
          try {
            fi = row.fieldIndex(col)
            val colval = row.get(fi)
            if (colval != null) {
              put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(hmd.family),
                org.apache.hadoop.hbase.util.Bytes.toBytes(col),
                BytesUtils.sparkRowToHBaseBytes(colval))
            }
          } catch {
            case e: Exception => {
              log.error(e)
            }
          }
        })

        table.put(put)
      } else if (optype.equals("U")) {

      } else if (optype.equals("D")) {
        val del = new org.apache.hadoop.hbase.client.Delete(rowkey)
        table.delete(del)
      } else {
        //
      }
    } catch {
      case e : Exception => {
        log.error(e)
      }
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    try { table.close() } catch {
      case e : Exception => log.error(e)
    }
    try { conn.close() } catch {
      case e : Exception => log.error(e)
    }
    if (errorOrNull!=null) { log.error(errorOrNull) }
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    val ret = try {
      hbaseconf = HBaseConfiguration.create()

      hbaseconf.set("hbase.client.pause", "50");
      hbaseconf.set("hbase.client.retries.number", "3");
      hbaseconf.set("hbase.rpc.timeout", "2000");
      hbaseconf.set("hbase.client.operation.timeout", "3000");
      hbaseconf.set("hbase.client.scanner.timeout.period", "10000");

      tableName = TableName.valueOf(hmd.tableName)
      conn = ConnectionFactory.createConnection(hbaseconf)
      table = conn.getTable(tableName)
      true
    } catch {
      case e : Exception => {
        log.error(e); false;
      }
    }

    ret
  }
}


class ComposedForeachWriter(fws:Array[ForeachWriter[Row]]) extends ForeachWriter[Row] {
  private val log = LogFactory.getLog(classOf[ComposedForeachWriter])

  override def open(partitionId: Long, epochId: Long): Boolean = {
    var ret = true

    for (fw <- fws) {
      ret = ret && fw.open(partitionId, epochId)
      if (!ret) {
        log.error(fw)
      }
    }

    ret
  }

  override def process(value: Row): Unit = {
    for (fw <- fws) {
      fw.process(value)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    for (fw <- fws) {
      fw.close(errorOrNull)
    }
  }
}
