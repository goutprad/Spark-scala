package com.spark.scala.accumulators

import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark drolls integration").master("local[1]").getOrCreate()
    val files = Option(new java.io.File("C:\\Users\\swaga\\Desktop\\Goutam Pradhan - Joining Kit").list()).get.filter(x=>x.endsWith(".pdf")).toList
    println(files.mkString("|")) 
    import spark.implicits._
    files.toDF().show()
  }
}