package com.spark.scala.accumulators

import org.apache.spark.util.AccumulatorV2
import java.util.Collections
import java.util
import scala.collection.JavaConversions._

class MapAccumulator extends AccumulatorV2[String, java.util.Map[String, Long]] {
  private val map = Collections.synchronizedMap(new util.HashMap[String, Long]())

  override def isZero: Boolean = map.isEmpty()

  override def copy(): MapAccumulator = {
    val newAcc = new MapAccumulator
    map.synchronized { newAcc.map.putAll(map) }
    newAcc
  }

  override def reset(): Unit = map.clear()

  override def add(key: String): Unit = map.synchronized { map.put(key, map.get(key) + 1L) }

  override def merge(other: AccumulatorV2[String, java.util.Map[String, Long]]): Unit = other match {
    case o: AccumulatorV2[String, java.util.Map[String, Long]] => for ((k, v) <- o.value) {
      val oldValue: java.lang.Long = map.get(k)
      if (oldValue != null) {
        map.put(k, oldValue.longValue() + v)
      } else {
        map.put(k, v)
      }
      println(s"key: ${k} oldValue: ${oldValue} newValue: ${v} finalValue: ${map.get(k)}")
    }
    case _ => throw new UnsupportedOperationException(s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: util.Map[String, Long] = map.synchronized {
    java.util.Collections.unmodifiableMap(new util.HashMap[String, Long](map))
  }

  def setValue(value: Map[String, Long]): Unit = {
    val newValue = mapAsJavaMap(value)
    map.clear()
    map.putAll(newValue)
  }
}