package com.spark.scala.accumulators

import org.apache.spark.sql.SparkSession

object MapAccumulatorTest extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark drolls integration").master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = Seq("A", "B", "C", "D", "A", "B", "D", "A", "C", "B").toDF("Letters")
    //df.show()

    val accum = new MapAccumulator
    spark.sparkContext.register(accum, "accum")
    accum.reset()
    df.foreach(x => {
      accum.add(x.getAs[String]("Letters"))
    })
    println(accum.value)

    accum.reset()
    val baseMap = collection.mutable.Map[String, Long]()
    baseMap.put("A", 10L)
    baseMap.put("B", 10L)
    accum.setValue(Map[String, Long](baseMap.toSeq: _*))
    
    df.foreach(x => {
      accum.add(x.getAs[String]("Letters"))
    })
    println("After using Accumulator Set Value: "+accum.value)
  }
}