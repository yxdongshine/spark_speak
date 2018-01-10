package com.ecaray.spark_sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by YXD on 2018/1/10.
 */
object WordCountSSql {
  case class Word (
                    content:String
                   )
  def main(args: Array[String]) {
    //创建配置
    val conf = new SparkConf()
      .setAppName("WordCountSSql")
      //.setMaster("local[*]")
      .setMaster("spark://192.168.9.109:7070")
      .set("spark.memory.fraction","0.75")
    //创建上下文
    val sc =  SparkContext.getOrCreate(conf)
    //初始化sparksql环境
    val sqlContext = SQLContext.getOrCreate(sc)
    //引进隐式转换
    import sqlContext.implicits._
    //val data = Array("0000-00-00 0000-00-00")
    //加载文件形成DataFrame
    val logDf: DataFrame = sc
      .textFile("/opt/data/data.txt")
      //.parallelize(data)
      .flatMap(_.split(" "))
      .map(
        c => Word(c)
      )
      .toDF()
    //注册成临时表
    logDf.registerTempTable("word")
    //统计操作
    val resultDf: DataFrame = sqlContext.sql(
      "SELECT content,COUNT(w.content) AS num " +
      "FROM word AS w WHERE LENGTH(w.content) > 0 " +
      "GROUP BY w.content " +
      "HAVING count(content) > 0 ")
    //输出
    resultDf.show()

    Thread.sleep(Long.MaxValue)
  }
}
