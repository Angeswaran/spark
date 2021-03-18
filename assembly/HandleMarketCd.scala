package com.humira.dataingestion

import org.apache.hadoop.fs.Path
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object HandleMarketCd {

  def main(arg: Array[String]) {
    val spark = SparkSession.builder().appName("HandleMarketCd").getOrCreate()
    //var sparkConf = new SparkConf().setAppName("HandleMarketCd")
    //var sc = new SparkContext(sparkConf)
    var sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = spark.sqlContext
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    //import sqlContext.implicits._
    import spark.implicits._

    val inputPath = arg(0)
    
    val outputPath = inputPath + "_temp"

    val inputDF = sqlContext.read.parquet(inputPath)

    val outputDF = inputDF.drop(inputDF("market_cd"))
    outputDF.write.parquet(outputPath)
    fs.delete(new Path(inputPath), true)
    fs.rename(new Path(outputPath),new Path(inputPath))
  }
}