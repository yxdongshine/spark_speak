package com.ecaray.spark_sql

import java.util.Properties

import com.ecaray.spark_sql.TraSSql.Word
import org.apache.spark.sql.{SaveMode, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by YXD on 2018/1/11.
 */
object TraToMysqlSSql {
  case class Word (
                    content:String
                    )
  def main(args: Array[String]) {
    //创建配置
    val conf = new SparkConf()
      .setAppName("TraToMysqlSSql")
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
    val wDf: DataFrame = sqlContext
      .read
      .load("/opt/data/wordParquet")

    //写出到mysql数据库
    //定义链接属性
    val  (url,user,password,driver) =("jdbc:mysql://192.168.9.86:3306/test", "root", "123456","com.mysql.jdbc.Driver")

    val pros = new Properties()
    pros.put("user", user)
    pros.put("password",password)
    pros.put("driver",driver)
    wDf
      .select("content")
      .write
      .mode(SaveMode.Overwrite)//覆盖
      .jdbc(url,"word",pros)


  }


}
