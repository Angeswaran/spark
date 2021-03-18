package com.humira.dataingestion

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
import com.humira.constants.{ CommonConstants, EnvConstants, DataQualityConstants }
import java.util.Properties
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.SparkSession

object ExpiryDatePreHandling {

  //val logger = Logger.getLogger(this.getClass);

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Data Restatement").getOrCreate()
    //val conf = new SparkConf().setAppName("Data Restatement")
    //val sc = new SparkContext(conf)
    val sc = spark.sparkContext
    val conf1 = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf1)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = spark.sqlContext
    //import sqlContext.implicits._
    import spark.implicits._
    val inputPath = args(0)
    val replacementValue = args(1)

    var ParquetDf = sqlContext.read.parquet(inputPath)

    val newRefinedDF = ParquetDf.withColumn("modified_date", when($"DELETION_FLAG" === "1", replacementValue).otherwise($"expiry_date"))

    val ReplaceDF = newRefinedDF.drop($"expiry_date").withColumnRenamed("modified_date", "expiry_date")

    val reOrderCol = ParquetDf.columns

    val reReplaceDF = ReplaceDF.select(reOrderCol.head, reOrderCol.tail: _*)
    val outputPath = inputPath + "_temp"

    reReplaceDF.write.parquet(outputPath)
    fs.delete(new Path(inputPath), true)
    fs.rename(new Path(outputPath), new Path(inputPath))
  }
}