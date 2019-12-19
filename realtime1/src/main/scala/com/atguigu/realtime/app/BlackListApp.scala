package com.atguigu.realtime.app

import java.util

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-09-27 10:06
  */
object BlackListApp {
    def statBlackList(spark: SparkSession, adsInfoDS: Dataset[AdsInfo]) = {
        import spark.implicits._
        // 1. 先过滤掉黑名单用户的点击记录
        val filteredAdsInfoDS: Dataset[AdsInfo] = adsInfoDS.mapPartitions(it => {
            val adsInfoList: List[AdsInfo] = it.toList
            val client: Jedis = RedisUtil.getJedisClient
            // 黑名单
            val blacList: util.Set[String] = client.smembers(s"blacklist:${adsInfoList(0).dayString}")
            client.close()
            // 过滤
            adsInfoList.filter(adsInfo => !blacList.contains(adsInfo.userId)).toIterator
        })
        
        // 2. 重新统计黑名单(已经进入黑名单的不会重新计算)
        filteredAdsInfoDS.createOrReplaceTempView("adsInfo")
        spark.sql(
            """
              |select
              | dayString,
              | userId
              |from adsInfo
              |group by dayString, userId, adsId
              |having count(*)>=1000000
            """.stripMargin)
            .writeStream
            .outputMode("update")
            .foreach(new ForeachWriter[Row] {
                var client: Jedis = _
                
                // 建立连接
                override def open(partitionId: Long, epochId: Long): Boolean = {
                    client = RedisUtil.getJedisClient
                    client != null && client.isConnected
                }
                
                // 写出数据
                override def process(value: Row): Unit = {
                    val day = value.getString(0)
                    val userId = value.getString(1)
                    
                    client.sadd(s"blacklist:$day", userId)
                }
                
                // 关闭连接
                override def close(errorOrNull: Throwable): Unit = {
                    if (client != null && client.isConnected) {
                        client.close()
                    }
                }
            })
            .trigger(Trigger.ProcessingTime(10000))
            .start()
        
        filteredAdsInfoDS
        
    }
}
