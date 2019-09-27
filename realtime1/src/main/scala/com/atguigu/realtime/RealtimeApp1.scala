package com.atguigu.realtime

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.app.BlackListApp
import com.atguigu.realtime.bean.AdsInfo
import org.apache.hadoop.mapred.ClusterStatus.BlackListInfo
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Author lzc
  * Date 2019-09-27 09:12
  */
object RealtimeApp1 {
    def main(args: Array[String]): Unit = {
        // 1. 先从kafka消费数据
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("RealtimeApp1")
            .getOrCreate()
        import spark.implicits._
        
        val dayFormatter = new SimpleDateFormat("yyyy-MM-dd")
        val hmFormatter = new SimpleDateFormat("HH:mm")
        
        val adsInfoDS: Dataset[AdsInfo] = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9092")
            .option("subscribe", "ads_log")
            .load
            .select("value")
            .as[String]
            .map(line => {
                val arr: Array[String] = line.split(",")
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
            
        
        //2. 需求1: 黑名单
        val filteredAdsInfoDS = BlackListApp.statBlackList(spark, adsInfoDS)
        
        // 需求2:
        
    }
}
