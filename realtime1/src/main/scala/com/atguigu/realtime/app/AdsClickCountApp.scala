package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-09-27 11:29
  */
object AdsClickCountApp {
    def statClickCount(spark: SparkSession, filteredAdsInfoDS: Dataset[AdsInfo]) = {
        val resultDF: DataFrame = filteredAdsInfoDS
            .groupBy("dayString", "area", "city", "adsId")
            .count()
        
        resultDF.writeStream
            
            .outputMode("complete")
            .foreachBatch((df, bachId) => {
                
                df.persist()
                if (df.count() > 0) {
                    
                    println("df count > 0")
                    df.foreachPartition(rowIt => {
                        val client: Jedis = RedisUtil.getJedisClient
                        var dayString: String = ""
                        val hashValue: Map[String, String] = rowIt.map(row => {
                            dayString = row.getString(0)
                            val area = row.getString(1)
                            val city = row.getString(2)
                            val adsId = row.getString(3)
                            val count: Long = row.getLong(4)
                            (s"$area:$city:$adsId", count.toString)
                        }).toMap
                        import scala.collection.JavaConversions._
                        if (hashValue.size > 0)
                            client.hmset(s"date:area:city:ads:$dayString", hashValue)
                        client.close()
                    })
                }
                df.unpersist()
            })
            .trigger(Trigger.ProcessingTime(10000))
            .start
            .awaitTermination()
    }
}
