package com.example.streaming.demo.data

import java.io.ByteArrayOutputStream

import com.example.streaming.demo.avro.{columns, ot1}
import com.example.streaming.pipeline.{AvroDataPlainConverter, AvroSchemaPlainConverter}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificDatumReader

/**
  * Created by yilong on 2019/3/23.
  */
object AvroDataBuilder {
  val composedSchemaStr = "{\"namespace\": \"com.example.streaming.demo.avro\",\"type\":\"record\",\"name\":\"ot1\"," +
    "\"fields\":[{\"name\":\"table\",\"type\":\"string\"}," +
    "{\"name\":\"op_type\",\"type\":\"string\"}," +
    "{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"columns\"," +
    "\"fields\":["+
    "{\"name\":\"nick\",\"type\":[\"string\",\"null\"],\"default\":\"null\"}," +
    "{\"name\":\"grade\",\"type\":[\"int\",\"null\"],\"default\":\"null\"}," +
    "{\"name\":\"uid\",\"type\":[\"string\",\"null\"],\"default\":\"null\"}]}],\"default\":\"null\"}," +
    "{\"name\":\"after\",\"type\":[\"columns\",\"null\"],\"default\":\"null\"}]}";

  val colSchemaStr = "{\"type\":\"record\",\"name\":\"columns\"," +
    "\"fields\":[{\"name\":\"grade\",\"type\":[\"int\",\"null\"],\"default\":\"null\"}," +
    "{\"name\":\"nick\",\"type\":[\"string\",\"null\"], \"default\":\"null\"}," +
    "{\"name\":\"uid\",\"type\":[\"string\",\"null\"],\"default\":\"null\"}]}"

  val simpleSchemaStr = "{" + "\"type\":\"record\"," +
    "\"name\":\"myrecord\"," +
    "\"fields\":[" +
    "  { \"name\":\"uid\", \"type\":\"string\" }," +
    "  { \"name\":\"nick\", \"type\":\"string\" }," +
    "  { \"name\":\"grade\", \"type\":\"int\" }" +
    "]}"

  val rowkeyedSchemaStr = "{" + "\"type\":\"record\"," +
    "\"name\":\"myrecordTarget\"," +
    "\"fields\":[" +
    "  { \"name\":\"rowkey\", \"type\":\"string\" }," +
    "  { \"name\":\"uid\", \"type\":\"string\" }," +
    "  { \"name\":\"nick\", \"type\":\"string\" }," +
    "  { \"name\":\"grade\", \"type\":\"int\" }" +
    "]}"

  def getComposedSchemaStr() = composedSchemaStr

  def getColSchemaStr() = colSchemaStr

  def getComposedSchema() : Schema = {
    val parser = new Schema.Parser();
    val scheam = parser.parse(composedSchemaStr)

    //println(scheam)
    scheam
  }

  def getColSchema() : Schema = {
    val parser = new Schema.Parser();
    val scheam = parser.parse(getColSchemaStr)

    //println(scheam)
    scheam
  }

  def getComposedData(id : Int) : Array[Byte] = {
    val ops = List("I","U","D")
    val schema = getComposedSchema

    val ot1 = new ot1()
    val bcol = new columns()
    val acol = new columns()

    ot1.setTable("table_"+id)
    ot1.setOpType(ops.apply(id%3))

    bcol.setGrade(id)
    bcol.setNick("nick_"+id)
    bcol.setUid("uid_"+id)

    acol.setGrade(10000+id)
    acol.setNick("nick_"+id)
    acol.setUid("uid_"+id)

    ot1.setAfter(acol)
    ot1.setBefore(bcol)

    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    datumWriter.write(ot1, encoder)
    encoder.flush()
    out.close()

    out.toByteArray
  }

  def getSimpleSchemaStr() = simpleSchemaStr
  def getSimpleSchema(): Schema = {
    val parser = new Schema.Parser();
    val scheam = parser.parse(simpleSchemaStr)

    //println(scheam)
    scheam
  }

  def getSimpleData(id:Int) : GenericData.Record = {
    val myrecord = new GenericData.Record(getSimpleSchema);
    myrecord.put("uid", "uid_" + id);
    myrecord.put("nick", "nick_" + id);
    myrecord.put("grade", id);

    myrecord
  }

  def getRowkeyedSchemaStr() = rowkeyedSchemaStr
  def getRowkeyedSchema(): Schema = {
    val parser = new Schema.Parser();
    val scheam = parser.parse(rowkeyedSchemaStr)

    //println(scheam)
    scheam
  }

  def getRowkeyedData(id:Int) : GenericData.Record = {
    val myrecord = new GenericData.Record(getRowkeyedSchema);
    myrecord.put("rowkey","rowkey_"+id)
    myrecord.put("uid", "uid_" + id);
    myrecord.put("nick", "nick_" + id);
    myrecord.put("grade", id);

    myrecord
  }

  def decodeAvroRecord(schema: Schema, message: Array[Byte]) : GenericRecord = {
    // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val userData: GenericRecord = reader.read(null, decoder)

    userData
  }

  def main(args: Array[String]): Unit = {
    val cdata = getComposedData(0)
    val sdata = getSimpleData(0)
    val ocdata = decodeAvroRecord(getComposedSchema(),cdata)
    println(ocdata)
    println(sdata)
    println(getRowkeyedData(0))
    //println(AvroSchemaConverters.toSqlType(getComposedSchema))
    //println(AvroSchemaConverters.toSqlType(getSimpleSchema))
    //println(AvroSchemaConverters.toSqlType(getRowkeyedSchema))
    val cplianschema = AvroSchemaPlainConverter.convert(getComposedSchema)
    val splianschema = AvroSchemaPlainConverter.convert(getSimpleSchema)
    println(cplianschema)
    println(splianschema)
    println(AvroDataPlainConverter.convert(ocdata, cplianschema))
  }
}
