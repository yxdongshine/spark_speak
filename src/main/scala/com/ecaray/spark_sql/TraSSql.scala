package com.ecaray.spark_sql

import com.ecaray.spark_sql.WordCountSSql.Word
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by YXD on 2018/1/11.
 */
object TraSSql {
  case class Word (
                    content:String
                    )
  def main(args: Array[String]) {
    //创建配置
    val conf = new SparkConf()
      .setAppName("TraSSql")
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
    val wDf: DataFrame = sc
      .textFile("/opt/data/data.txt")
      //.parallelize(data)
      .flatMap(_.split(" "))
      .map(
        c => Word(c)
      )
      .toDF()
    //写出到json文件
    wDf
      .select("content")
      .write
      .format("json")
      .save("/opt/data/wordJson")//路径

    //写出到parquet文件
    wDf
      .select("content")
      .write
      .format("parquet")
      .save("/opt/data/wordParquet")//路径

  }

}
