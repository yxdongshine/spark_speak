package com.ecaray.report

import com.ecaray.kafka.ReportProducer
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Random

/**
 * Created by YXD on 2018/5/4.
 */
object ReportTest {
  val DELIMITER: String = "#"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ReportTask")
      .setMaster("local[*]") //设置本地模式
    //.setMaster("spark://192.168.9.81:7070") //设置集群模式运行
    val args = Array("4@$12#SELECT COUNT(1) FROM tra_order WHERE date= 2018-05-03")
    var sql = ""
    var pos = ""
    if (args != null && args.length > 0) {
      val splitArr = args(0).split(DELIMITER)
      if(2 == splitArr.length){
        pos = splitArr(0)
        sql = splitArr(1)
      }
    }
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext);
    val result: DataFrame = sqlContext.read.text("E:\\test.txt")
    //发送出去
    val key = new Random().nextInt().toString
    var message = pos+DELIMITER
   /* if(0 < result.count()){
      val row = result.first()
      if(null != row){
        message += row.get(0)
      }
    }else{
      message += 0
    }*/

    //这里开始调用kafka消息
    ReportProducer.getReportProducerInstance.sendMessage(key,message)
  }

}
