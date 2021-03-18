package com.humira.dataingestion

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object ExplodeXML {
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
    
    val inputFile = arg(0)
    val inputString = """<books><book><details><detail><name>author</name><value>Collins, James</value></detail><detail><name>title</name><value>XML Developer's Guide</value></detail><detail><name>genre</name><value>Computer</value></detail><detail><name>price</name><value>44.95</value></detail><detail><name>publish_date</name><value>2000-10-01</value></detail><detail><name>description</name><value>An in-depth look at creating applications with XML.</value></detail></details></book><book><details><detail><name>author</name><value>Ralls, Kim</value></detail><detail><name>title</name><value>Midnight Rain</value></detail><detail><name>genre</name><value>Fantasy</value></detail><detail><name>price</name><value>5.95</value></detail><detail><name>publish_date</name><value>2000-12-16</value></detail><detail><name>description</name><value>A former architect battles corporate zombies, an evil sorceress, and her own childhood to become queen of the world.</value></detail></details></book></books>"""
    val inputrdd = sc.textFile(inputFile)
    inputrdd.foreach(println)
    val data = inputrdd.take(1)(0)
    println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"+data)
    val xmlValue = scala.xml.XML.loadString(inputString)
    println(xmlValue)
  }
  
}