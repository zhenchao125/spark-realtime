package com.atuigu.realtime2.app

import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

/**
  * Author lzc
  * Date 2019-09-27 14:31
  */
object AreaAdsClickTop3 {
    def statAearAdsClick(adsInfoDStream: DStream[AdsInfo]) = {
        val value: DStream[((String, String, String), Int)] = adsInfoDStream
            .map(adsInfo => ((adsInfo.dayString, adsInfo.area, adsInfo.adsId), 1))
            .updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
                Some(seq.sum + option.getOrElse(0))
            })
        
        
    }
}

/*
统计每天每地区每广告的点击量
((dayString, area, adsId), 1)




 */