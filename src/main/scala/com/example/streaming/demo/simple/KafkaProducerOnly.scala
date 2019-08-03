package com.example.streaming.demo.simple

import java.util.Properties

import com.example.streaming.demo.data.{AvroDataBuilder, KafkaDataBuilder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by yilong on 2019/3/17.
  */

object KafkaProducerOnly extends App {
  def sendAvroRecord(topic : String,
                 producer : KafkaProducer[String, Array[Byte]],
                 myrecord : Array[Byte]) : Unit = {
    println("***************************************")
    println(myrecord)

    val message = new ProducerRecord[String,Array[Byte]](topic, null, myrecord)
    val result = producer.send(message)
    println("********************** send result offset : "+result.get().offset())
  }

  def writeAvroRecord(props : Properties , topic : String, schemaStr : String, recordBuilder: (Int)=>Array[Byte] ) : Unit = {
    val parser = new Schema.Parser();
    val schema = parser.parse(schemaStr);
    println(schema)

    val producer = new KafkaProducer[String, Array[Byte]](props)

    val writer = new SpecificDatumWriter[GenericRecord](schema)

    (1001 to 2000).foreach(i => {
      sendAvroRecord(topic, producer, recordBuilder(i))
      //Thread.sleep(500)
    })

    producer.close();
  }

  val topic = "myrecord2"
  val clientid = "myrecord1.client.001"

  val props = KafkaDataBuilder.getKafkaProp()
  props.put("client.id", clientid)

  writeAvroRecord(props, topic, AvroDataBuilder.getComposedSchemaStr(), AvroDataBuilder.getComposedData)
}
