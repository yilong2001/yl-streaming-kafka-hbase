package com.example.streaming.demo.data

import java.util.Properties

/**
  * Created by yilong on 2019/3/24.
  */
object KafkaDataBuilder {
  def getKafkaProp(groupId : String, clientId : String) : Properties = {
    val props = new Properties();
    props.put("kafka.bootstrap.servers", "localhost:9092")
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")

    props.put("enable.auto.commit", "false")
    props.put("auto.offset.reset", "earliest")
    props.put("consumer.timeout.ms", "120000")
    props.put("auto.commit.interval.ms", "10000")

    //props.put("client.id", clientId)
    //if set client.id : throw javax.management.InstanceAlreadyExistsException: kafka.consumer:type=app-info
    props.put("group.id", groupId)
    props.put("application.id",clientId)

    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.mechanism", "PLAIN")

    props
  }

  def getKafkaProp() : Properties = {
    val props = new Properties();
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    props.put("message.send.max.retries", "5")
    props.put("request.required.acks", "-1")

    props.put("enable.auto.commit", "false")
    props.put("auto.offset.reset", "earliest")
    props.put("consumer.timeout.ms", "120000")
    props.put("auto.commit.interval.ms", "10000")

    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.mechanism", "PLAIN")

    props
  }
}
