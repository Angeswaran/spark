package com.humira.dataingestion
import org.apache.spark.AccumulatorParam

object StringAccumulator extends AccumulatorParam[String] {

  def zero(initialValue: String): String = {
    ""
  }

  def addInPlace(s1: String, s2: String): String = {
    s"$s1 $s2"
  }
}
  
  
