package com.example.streaming.pipeline

import java.io.ByteArrayOutputStream

import com.example.streaming.util.{MetaUtils, RowUtils}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
  * Created by yilong on 2019/4/6.
  */
object EnDecoder {
  def encodeAvroRecord(writer : SpecificDatumWriter[GenericRecord],
                       myrecord : GenericData.Record): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(myrecord, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  def encodeRowIntoAvroRecord(schema : Schema, cols : Array[String], row : org.apache.spark.sql.Row) : GenericData.Record = {
    val myrecord = new GenericData.Record(schema);

    cols.foreach(col => {
      myrecord.put(col, row.get(row.fieldIndex(col)))
    })

    myrecord
  }

  //TODO: cols is sorted
  def encodeRowIntoPlainDat(fieldSplit:String, fieldTerminate : String, cols : Array[String], row : org.apache.spark.sql.Row) : String = {
    null
  }

  def encodeAvroRecordIntoRow(record: GenericRecord, structType: StructType, tpot:TopicPartitionOffsetTimestamp)
  : org.apache.spark.sql.Row = {
    val rawSize = structType.fields.size
    val rawRow = AvroDataPlainConverter.convert(record, structType)
    val newSt = MetaUtils.appendTopicFieldsToStructType(structType)

    RowUtils.appendTopicValuesToRow(rawRow, tpot, newSt)
  }

}
