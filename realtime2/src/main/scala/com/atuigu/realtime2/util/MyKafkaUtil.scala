package com.atuigu.realtime2.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies

/**
  * Author lzc
  * Date 2019-09-27 14:13
  */
object MyKafkaUtil {
    val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "bigdata",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    
    
    def getKafkaStream(ssc:StreamingContext, topic: String): InputDStream[ConsumerRecord[String, String]] = {
        KafkaUtils.createDirectStream(
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
        )
    }
    
}
