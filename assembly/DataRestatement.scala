package com.humira.datavalidation

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
import com.humira.constants.{ CommonConstants, EnvConstants, DataQualityConstants }
import java.util.Properties
import com.humira.dataingestion.ReadPropeties
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{ col, lit, when }
import org.apache.spark.sql.SparkSession

object DataRestatement {

  def readProperties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  def readDQPropeties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  def readEnvpropeties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  val logger = Logger.getLogger(this.getClass);

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
    
    var propsDQ: Properties = readDQPropeties(args(0))
    var propsEnv: Properties = readEnvpropeties(args(2))
    // var feedname = propsDQ.getProperty(DataQualityConstants.feedName)
    val feedLoadType = sc.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/loadmetadata.txt").take(1)(0).split("\\,")(2)
    if (feedLoadType.equalsIgnoreCase("I")) {
      var inSchemePath = propsEnv.getProperty(CommonConstants.INCOMINGSCHEMAPATH) + "/" + propsDQ.getProperty(CommonConstants.INCOMINGSCHEMAFILE)
      var masterSchema = sc.textFile(inSchemePath).take(1)(0).toLowerCase

      if (masterSchema.contains("deletion_flag")) {
        var feedname = propsDQ.getProperty(CommonConstants.RFDTBLLOCATION).split("=")(1)

        val oldfeedmetadata = sc.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/" + feedname + "_refinedmetadata.txt").take(1).mkString("|")

        if (oldfeedmetadata.isEmpty || oldfeedmetadata == null) {
          logger.error("No lines found in the feedmetadata file ..")
          throw new RuntimeException("No lines found in the feedmetadata file ..")
        }
        val feedMetadataArr = oldfeedmetadata.split('|')
        // val date_month = feedMetadataArr(0)
        val ingestion_dt_tm = feedMetadataArr(1)

        val format = new SimpleDateFormat("MM/dd/yyyy")
        var currentdate = format.format(Calendar.getInstance().getTime())

        val tableLocation = propsDQ.getProperty(CommonConstants.RFDTBLLOCATION).split("=")
        val tablePathComplete = propsEnv.getProperty(EnvConstants.REFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocation(1)
        val tablePathCompleteWithPartitions = tablePathComplete + CommonConstants.FILE_SEPARATOR + ingestion_dt_tm+"/deletion_flag=1"

        var ParquetDf = sqlContext.read.parquet(tablePathCompleteWithPartitions)

        val newRefinedDF = ParquetDf.withColumn("modified_date", lit(currentdate))

        val ReplaceDF = newRefinedDF.drop($"expiry_date").withColumnRenamed("modified_date", "expiry_date")

        val reOrderCol = ParquetDf.columns

        val reReplaceDF = ReplaceDF.select(reOrderCol.head, reOrderCol.tail: _*)
        
        val outputPath = tablePathCompleteWithPartitions + "_temp"

        reReplaceDF.write.parquet(outputPath)
        fs.delete(new Path(tablePathCompleteWithPartitions), true)
        fs.rename(new Path(outputPath), new Path(tablePathCompleteWithPartitions))

        //if (fs.exists(new Path(tablePathCompleteWithPartitions)))

          //fs.delete(new Path(tablePathCompleteWithPartitions), true)

        //reReplaceDF.write.parquet(tablePathCompleteWithPartitions)
      }
    }

  }
}