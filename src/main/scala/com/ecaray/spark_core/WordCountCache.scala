package com.ecaray.spark_core

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by YXD on 2017/12/19.
 * 提交任务脚本：
 * bin/spark-submit \
--master spark://192.168.9.109:7070 \
--deploy-mode cluster \
--driver-memory 500m \
--executor-memory 1536m \
--class  com.ecaray.spark_core.WordCountCache \
--conf  "spark.ui.port=5050" \
/opt/data/spark-1.0-SNAPSHOT-jar-with-dependencies.jar
 */
object WordCountCache {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("WordCountCache")
      // .setMaster("local[*]")
      .setMaster("spark://192.168.9.109:7070")
    val sc = new SparkContext(conf)
    val textfilerdd = sc.textFile("/opt/data/20171025-stif_act.sql")
      .filter(_.length>0)
      .flatMap(
        (_.split(" ").map((_,1)) ))

    //rdd缓存
    textfilerdd.cache()
    //job1
    println(textfilerdd.count())
    //job2
    textfilerdd
      .reduceByKey(_+_)
      .foreach(println)

    Thread.sleep(Long.MaxValue)
  }


}
