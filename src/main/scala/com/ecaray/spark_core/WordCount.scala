package com.ecaray.spark_core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by YXD on 2017/12/19.
 */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("WordCount")
      //.setMaster("local[*]") 设置本地模式
      //.setMaster("spark://192.168.9.81:7070") //设置集群模式运行

    val sc = new SparkContext(conf)
    val textfilerdd = sc.textFile("/app/data/data.txt")//节点路径 如果本机运行指定相应本地文件

    textfilerdd
      .filter(_.length>0)
      .flatMap(
        (_.split(" ").map((_,1)) ))
      .reduceByKey(_+_)
      .foreach(println)

    Thread.sleep(Long.MaxValue)
  }
}
