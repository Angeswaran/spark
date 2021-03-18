package com.humira.dataingestion

import scala.util.Try
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import java.util.Properties
import java.io.BufferedWriter;
import java.io.OutputStreamWriter
//import org.apache.spark.sql.hive.spark
import org.apache.spark.sql.SparkSession


import com.humira.constants.CommonConstants
import com.humira.utilities.CustomLogger
import com.humira.constants.CommonConstants

class RenameFiles {

  def readPropeties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  var readProp: Properties = null
  val spark = SparkSession.builder().appName("FileValidation").getOrCreate()
  //var sparkConf = new SparkConf().setAppName("FileValidation")
  var sparkContext = spark.sparkContext
  val conf = sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  //val spark = new spark(sparkContext)

  /**
   * creating the logger instance
   */
  val logger = new CustomLogger().getLogger(this.getClass.toString())

  def main1(arg: Array[String]) {

    System.out.println("**********************************************Rename Files")

    readProp = readPropeties(arg(1))
    var finalFilesForControlFile = collection.mutable.ArrayBuffer[String]()
    val landingBasePath = readProp.getProperty(CommonConstants.LANDINGBASEPATH)
    val CotrolFileAbsolutePath = readProp.getProperty(CommonConstants.CONTROLFILELANDINGPATH)
    val delimeter = readProp.getProperty(CommonConstants.CONTROLFILEDELIMITER)
    val dataSet = readProp.getProperty(CommonConstants.DATASET)
    val dataSetTbl = readProp.getProperty(CommonConstants.DATASETTBL)
    val dataSetFile = readProp.getProperty(CommonConstants.DATASETFILE)
    var auditfileOutputPath = readProp.getProperty(CommonConstants.AUDITFILEBASEPATH)
    val metaDataLoc = readProp.getProperty(CommonConstants.METADATALOCATION)
    val randomNum = scala.util.Random
    val batchID = randomNum.nextInt(999999).toString()
    var metaData = batchID
    val filesInLanding = fs.listFiles(new Path(landingBasePath), true)
    logger.info("Dataset Name:" + dataSet)
    logger.info("dataSet Table Name:" + dataSetTbl)
    logger.info("dataSet File Name:" + dataSetFile)
    logger.info("Landing Base Path:" + landingBasePath)
    logger.info("Cotrol File Absolute Path:" + CotrolFileAbsolutePath)
    while (filesInLanding.hasNext()) {
      var fileNameForTableComp = ""
      val fileName = filesInLanding.next().getPath().getName()
      val fileNameCompare = fileName.split("\\.")
      val fileNameArray = fileNameCompare(0).split("\\_")
      var date_month_End = fileNameArray.reverse(0)
      if (date_month_End.equals("2")) {
        date_month_End = "ignoreText"
      }
      val date_month_Start = fileNameArray(0)
      var fileNameChanged = false
      var finalFileNameArray = collection.mutable.ArrayBuffer[String]()
      var finalFileName = ""
      if (Try(date_month_Start.toDouble).isSuccess == true) {
        for (element <- 1 to fileNameArray.length - 1) {
          finalFileNameArray += fileNameArray(element)
        }
        fileNameForTableComp = finalFileNameArray.mkString("_")
        finalFileNameArray += date_month_Start
        finalFileName = finalFileNameArray.mkString("_")
        finalFileName = finalFileName + "." + fileNameCompare(1)
        fileNameChanged = true
      } else if (Try(date_month_Start.toDouble).isSuccess == false && Try(date_month_End.toDouble).isSuccess == false) {
        val format = new SimpleDateFormat("yyyyMMdd")
        val currentdate = format.format(Calendar.getInstance().getTime())
        finalFileName = fileNameCompare(0) + "_" + currentdate + "." + fileNameCompare(1)
        fileNameForTableComp = fileNameCompare(0)
        fileNameChanged = true
      } else {
        logger.info("File Name has DateMonth at last position")
        finalFileName = fileName
        val fileNameArray = fileName.split("\\.")(0).split("\\_")
        fileNameForTableComp = fileNameArray.dropRight(1).mkString("_")
      }
      logger.info("File Name from Directory: " + fileNameForTableComp)
      logger.info("Final File Name from Directory: " + finalFileName)
      if (dataSetFile.equalsIgnoreCase(fileNameForTableComp)) {
        finalFilesForControlFile += finalFileName + delimeter + delimeter + delimeter + dataSetTbl.toUpperCase() + " Table" + delimeter + readProp.getProperty(CommonConstants.LOADTYPE)
      }
      if (fileNameChanged) {
        val intialFilePath = new Path(landingBasePath + "/" + fileName)
        val finalFilePath = new Path(landingBasePath + "/" + finalFileName)
        fs.rename(intialFilePath, finalFilePath)
      }
    }
    val contrilFilePath = new Path(CotrolFileAbsolutePath)
    var outStream = fs.create(contrilFilePath, true)
    val controlFileString = finalFilesForControlFile.mkString("\n")
    outStream.writeBytes(controlFileString)
    outStream.close()
    val ingestionDateAudit = new java.text.SimpleDateFormat("dd-MM-yyyy").format(new java.util.Date())
    auditfileOutputPath = auditfileOutputPath + "/load_date=" + ingestionDateAudit
    metaData = metaData + "," + ingestionDateAudit + "," + "F" + "," + dataSet

    //writing batchID and load date as meta data. to be read by RAW,DQ,Refind for audit file updation
    val os = fs.create(new Path(metaDataLoc + "/loadmetadata.txt"))
    os.writeBytes(metaData)
    os.close()
    spark.sql(readProp.getProperty(CommonConstants.DetailedFileLocation))
    spark.sql(readProp.getProperty(CommonConstants.DetailedAuditDB))
    spark.sql(readProp.getProperty(CommonConstants.DetailedAuditTable))

    //spark.sql(readProp.getProperty(CommonConstants.DTLAUDITTBLDB))
    spark.sql(readProp.getProperty(CommonConstants.CREATEDETAILAUDITTABLE))
    spark.sql(readProp.getProperty(CommonConstants.DTLMSCK))
  }
}