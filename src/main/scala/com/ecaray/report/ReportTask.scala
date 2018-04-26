package com.ecaray.report

import com.ecaray.kafka.ReportProducer
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by YXD on 2018/4/26.
 */
object ReportTask {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ReportTask")
    var sql = ""
    if (args != null && args.length > 0) {
      sql = args(0)
    }
    val sparkContext = SparkContext.getOrCreate(conf)
    //集成hiveContext
    val hiveContext =  new HiveContext(sparkContext)
    //执行sql
    val result:DataFrame = hiveContext.sql(sql)
    //这里开始调用kafka消息
    result.foreach(f = row => {
      //发送出去
      val key = new Random().nextInt().toString
      val message = row.get(0).toString
      ReportProducer.getReportProducerInstance.sendMessage(key,message)
    })
  }

}
