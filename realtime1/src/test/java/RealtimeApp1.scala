import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-09-27 09:12
  */
object Test {
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
        
        import org.apache.spark.sql.functions._
        val adsInfoDS = spark.readStream
            .format("socket")
            .option("host", "hadoop201")
            .option("port", 20000)
            .load
            .select("value")
            .as[String]
            .map(line => {
                val arr: Array[String] = line.split(",")
                (Timestamp.valueOf(arr(0)), arr(1))
            })
            .toDF("ts", "word")
            .withWatermark("ts", "2 minutes")
            .groupBy("ts", "word")
            .count()
            .writeStream
            .format("console")
            .outputMode("update")
            .start
            .awaitTermination()
        
        //2. 需求1: 黑名单
    }
}
