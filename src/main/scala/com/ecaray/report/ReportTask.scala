package com.ecaray.report

import com.ecaray.kafka.ReportProducer
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.util.Random

/**
 * Created by YXD on 2018/4/26.
 */
object ReportTask {
  val DELIMITER: String = "#"
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ReportTask")
      .set("spark.rpc.askTimeout","600s")
      .set("spark.network.timeout","600s")
      .set("spark.executor.heartbeatInterval","600s")
      .set("spark.default.parallelism","30")
      .set("spark.storage.memoryFraction","0.2")
      .set("spark.shuffle.memoryFraction","0.6")
      .set("spark.shuffle.file.buffer","256k")
      .set("spark.reducer.maxSizeInFlight","128m")
      .set("spark.shuffle.manager","hash")
      .set("spark.shuffle.consolidateFiles","true")
    //.setMaster("local[*]") //设置本地模式
    //.setMaster("spark://192.168.9.81:7070") //设置集群模式运行
    //val args = Array("4@$12#SELECT COUNT(1) FROM tra_order WHERE date= 2018-05-03")
    var sql = ""
    var pos = ""
    if (args != null && args.length > 0) {
      //先将前后双引号去掉
      if(args(0).length > 2){
        args(0) = args(0).substring(1,args(0).length-1)
        val splitArr = args(0).split(DELIMITER)
        if(2 == splitArr.length){
          pos = splitArr(0)
          sql = splitArr(1)
        }
      }
    }
    val sparkContext =  SparkContext.getOrCreate(conf)
    //集成hiveContext
    val hiveContext =  new HiveContext(sparkContext)
    //使用数据库
    hiveContext.sql("use report")
    //发送出去
    val key = new Random().nextInt().toString
    var message = pos+DELIMITER
    ReportProducer.getReportProducerInstance.sendMessage(key,message+sql)
    //执行sql
    val result:DataFrame = hiveContext.sql(sql)

    /* if(0 < result.count()){
      val row = result.first()
      if(null != row){
        message += row.get(0)
      }
    }else{
      message += 0
    }*/
   val row = result.first()
   if(null != row){
     message += row.get(0)
   }
   /* var num = 0L
    result.foreachPartition( iter =>{
     iter.foreach(
       row =>{
         num += row.getLong(0)
      }
     )
    })
    message += num*/
   //这里开始调用kafka消息
   ReportProducer.getReportProducerInstance.sendMessage(key,message)
   //Thread.sleep(Long.MaxValue)
  }

}
