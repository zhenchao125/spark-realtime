package com.atuigu.realtime2

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.bean.AdsInfo
import com.atuigu.realtime2.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-09-27 14:11
  */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        
        val dayFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val hmFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")
        val adsInfoDStream: DStream[AdsInfo] =  MyKafkaUtil.getKafkaStream(ssc, "ads_log").map(record => {
            val arr: Array[String] = record.value().split(",")
            val date: Date = new Date(arr(0).toLong)
            AdsInfo(
                arr(0).toLong,
                new Timestamp(arr(0).toLong),
                dayFormatter.format(date),
                hmFormatter.format(date),
                arr(1),
                arr(2),
                arr(3),
                arr(4)
            )
        })
        // 1. 需求1: 每天每地区热门广告 Top3
        
        ssc.start()
        ssc.awaitTermination()
    }
}
/*

 */