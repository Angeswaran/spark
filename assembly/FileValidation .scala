package com.humira.dataingestion

import com.humira.constants.CommonConstants
import com.humira.utilities.CustomLogger

import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.hadoop.fs.Path
//import org.apache.spark.SparkConf
//import org.apache.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import java.io.{ IOException, FileOutputStream, FileInputStream, File }
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }
import java.io.BufferedInputStream
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.annotation.RegEx
import scala.util.matching.Regex
import java.util.{ Calendar, Properties }
import scala.collection.JavaConversions._
//import java.util.zip.ZipInputStream
import java.io.{ BufferedInputStream, FileInputStream, FileOutputStream }
import java.util.zip.{ ZipEntry, ZipOutputStream }
import scala.sys.process._
import org.apache.spark.sql.SparkSession

/**
 * *****************************************************************************************
 *  @Author By CTS BIGDATA TEAM
 *  @Version 2.10.5
 *  @Description: Following steps are included in this class:
 *
 *         1. It does 3 levels of file validation and logs all the info in audit table.
 * a. It check s if the file is present in landing location.
 * b. It compares the record count from the control file and the count calculated in the code.
 * c. file naming pattern, based on the data set and vedor that we pass as arguments.
 * This check is done against reference file file-pattern-reference.txt
 * 2. If all three checks are passed, file is marked as G (good file), else B (bad file) in detailed audit file.
 * 3. If validation for all the files in control file are successful, then it proceeds with the load, else if
 * even one file fails the validation, it would stop the whole load. and users will be notified through mail.
 *
 * **************************************************************************************
 */

class FileValidation {

  val ingestionDate = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new java.util.Date())
  val ingestionDateAudit = new java.text.SimpleDateFormat("dd-MM-yyyy").format(new java.util.Date())
  var readProp: Properties = null
  var fileNamePatternRDD: RDD[String] = null
  var fileNameRefLen: Int = 0
  var fileExtensionCurr = ""
  var fileNameCurr = ""
  var fileNameRefArr: Array[String] = null
  var fileNameCurrLen = 0
  var partFileNameCurr = ""
  var goodToLoadFlag = false
  var auditDBEnv = ""
  var auditTBLNameEnv = ""
  var auditTBLLocEnv = ""
  val auditFileInfo = collection.mutable.ArrayBuffer[String]()
  var goodToLoadFlagArray = collection.mutable.ArrayBuffer[Boolean]()

  def readPropeties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  /**
   * creating the logger instance
   */
  val logger = new CustomLogger().getLogger(this.getClass.toString())

  /**
   * Set Spark and Hive Configurations
   */
  val spark = SparkSession.builder().appName("FileValidation").getOrCreate()
  val sc= spark.sparkContext
  //var sparkConf = new SparkConf().setAppName("FileValidation")
  //var sparkContext = new SparkContext(sparkConf)
  val conf = sc.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  //val hiveContext = new HiveContext(sparkContext)

  /**
   * Stop Spark Context
   */
  def stopSparkContext() {
    println("Tried to kill the spark job gracefully")
    spark.stop()

  }

  /**
   * Gets the file name pattern from reference file
   */
  def getFilePattern(dataSet: String, vendor: String, feedName: String): String = {
    var fileNamePattern = ""
    /*
    Step1 - filters line having combination of "dataSet", "vendor", "feedName".
    Step2 - takes the record and using mkString converts to string containing only recs whcih satisfies the previous condition.
    */
    try {
      val fileNamePatternRecRDD = fileNamePatternRDD.filter { x => x.split("\\|")(0).equals(dataSet) && x.split("\\|")(1).equals(vendor) && x.split("\\|")(2).equals(feedName) } //.take(1).mkString("|")
      val countNo = fileNamePatternRecRDD.count

      fileNamePattern = fileNamePatternRecRDD.take(1).mkString("|")

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    fileNamePattern
  }

  def main1(arg: Array[String]) {

    System.out.println("**********************************************FileValidation")

    readProp = readPropeties(arg(1))
    val controlFilePath = readProp.getProperty(CommonConstants.CONTROLFILEPATH)
    val landingBasePath = readProp.getProperty(CommonConstants.LANDINGBASEPATH)
    val dataSet = readProp.getProperty(CommonConstants.DATASET)
    val vendor = readProp.getProperty(CommonConstants.VENDOR)
    var auditfileOutputPath = readProp.getProperty(CommonConstants.AUDITFILEBASEPATH)

    val metaDataLoc = readProp.getProperty(CommonConstants.METADATALOCATION)

    //creation of Batch_id
    val randomNum = scala.util.Random
    val batchID = randomNum.nextInt(999999).toString()
    var metaData = batchID
    var feedLoadType = "I"
    var FeedTypeIdentifierDIAGNOSIS = 0
    var FeedTypeIdentifierACTIVITY = 0
    var FeedTypeIdentifierPROCEDURE = 0

    //get file name pattern in RDD
    fileNamePatternRDD = sc.textFile(readProp.getProperty(CommonConstants.FILENAMEPATTERNREFFILE))
    fileNamePatternRDD.cache()

    //get the control file, with all feed and file lists to be processed
    val controlFileRDD = sc.textFile(controlFilePath)
    val splitString = readProp.getProperty(CommonConstants.CONTROLFILEDELIMITER)
    val delimeter = "\\" + splitString

    // val controlFileArray = controlFileRDD.collect().drop(2).dropRight(2) //collect RDD into an Array

    val controlFileArray = controlFileRDD.collect()

    //for (col <- 0 to controlFileArray.length - 1) {
    for (col <- controlFileArray.par) {
      var fileNameRefArr: Array[String] = null
      var fileNamePattern = ""
      var fileExtensionRef = ""
      var fileNameRef = ""
      var partFileNameRef = ""
      var fileName = ""
      var RecCountReceived = ""
      var feedName = ""
      val index1 = controlFileArray.indexOf(col)
      // get all the required info from list file
      val fileInfoArray = controlFileArray(index1).split(delimeter)

      fileName = fileInfoArray(0)
      RecCountReceived = fileInfoArray(1)
      feedName = fileInfoArray(3).split(" ")(0)
      if (dataSet.equalsIgnoreCase("PTD")) {
        if (feedName.equalsIgnoreCase("PTD_DIAGNOSIS")) {
          if (fileInfoArray(4).equalsIgnoreCase("F")) {
            FeedTypeIdentifierDIAGNOSIS = 1
          }
        }
        if (feedName.equalsIgnoreCase("PTD_PATIENT_ACTIVITY")) {
          if (fileInfoArray(4).equalsIgnoreCase("F")) {
            FeedTypeIdentifierACTIVITY = 1
          }
        }
        if (feedName.equalsIgnoreCase("PTD_PROCEDURE")) {
          if (fileInfoArray(4).equalsIgnoreCase("F")) {
            FeedTypeIdentifierPROCEDURE = 1
          }
        }
      } else {
        if (fileInfoArray(4).equalsIgnoreCase("F")) {
          feedLoadType = "F"
        } else {
          feedLoadType = "I"
        }
      }



      val fileNameArray = fileName.split("\\.")
      val noGZExtnFilename: String = {

        if (fileName.endsWith(".gz")) {
          val noGZExtnFilename = fileNameArray.dropRight(1).mkString(".")
          noGZExtnFilename
        } else {
          val txtadd = ".txt"
          val noGZExtnFilename = fileNameArray.dropRight(1).mkString(".") + txtadd
          noGZExtnFilename
        }
      }

      var txn_month = fileNameArray(0).split("\\_").reverse(0)
      var date_month = fileNameArray(0).split("\\_").reverse(1)

      //below block checks if only date month has come in the file name
      if (Try(date_month.toDouble).isSuccess == false) {
        date_month = txn_month
        txn_month = ""
      }

      var landToRawFailReason = ""
      var fileExistsFlag: Boolean = true
      //  var recordCount = ""

      var landingFile = ""
      landingFile = landingBasePath + "/" + fileName
      logger.info("File to be Validated - " + landingFile)

      var recordCount = {
        if (fileName.endsWith(".zip")) {
          val filenametxt = fileInfoArray(0).split("\\.").dropRight(1).mkString(".") + ".txt"
          var recordCount = ""

          if (fs.exists(new Path(landingFile))) {
            Seq("hdfs", "dfs", "-cat", landingBasePath + "/" + fileName) #| Seq("gzip", "-d") #| Seq("hdfs", "dfs", "-put", "-", landingBasePath + "/" + filenametxt) !

            val textfile = sc.textFile(landingBasePath + "/" + filenametxt)
            var recordCount2 = textfile.count()
            if (readProp.getProperty(CommonConstants.COUNTINCLUDESHEADER).equalsIgnoreCase("false")) {
              //Removing 1 from total count for header row as control file count ignore it for Epsilon Longitudinal dataset
              recordCount2 = recordCount2 - 1
            }
            
            val recordCount3 = recordCount2.toInt
            
            recordCount = recordCount3.toString()

          } else {
            val landingFileRDD = sc.textFile(landingBasePath + "/" + filenametxt)
            var recordCount1 = landingFileRDD.count()
            if (readProp.getProperty(CommonConstants.COUNTINCLUDESHEADER).equalsIgnoreCase("false")) {
              //Removing 1 from total count for header row as control file count ignore it for Epsilon Longitudinal dataset
              recordCount1 = recordCount1 - 1
            }
            recordCount = recordCount1.toString()
          }
          recordCount
        } else {

          //println("NO ZIP OR GZ FOUND & IN SIDE ELSE CONDITION")
          var recordCount = ""
          if (fs.exists(new Path(landingFile))) {
            val landingFileRDD = sc.textFile(landingFile)
            var recordCount1 = landingFileRDD.count()
            if (readProp.getProperty(CommonConstants.COUNTINCLUDESHEADER).equalsIgnoreCase("false")) {
              //Removing 1 from total count for header row as control file count ignore it for Epsilon Longitudinal dataset
              recordCount1 = recordCount1 - 1
            }

            recordCount = recordCount1.toString()
          } else {
            val filenamezip = fileInfoArray(0).split("\\.").dropRight(1).mkString(".") + ".zip"

            Seq("hdfs", "dfs", "-cat", landingBasePath + "/" + filenamezip) #| Seq("gzip", "-d") #| Seq("hdfs", "dfs", "-put", "-", landingBasePath + "/" + fileName) !

            val textfile = sc.textFile(landingBasePath + "/" + fileName)
            var recordCount2 = textfile.count()
            if (readProp.getProperty(CommonConstants.COUNTINCLUDESHEADER).equalsIgnoreCase("false")) {
              //Removing 1 from total count for header row as control file count ignore it for Epsilon Longitudinal dataset
              recordCount2 = recordCount2 - 1
            }
            val recordCount3 = recordCount2.toInt
            recordCount = recordCount3.toString()
          }
          recordCount
        }
      }
      

      try {
        val landingFileRDD = sc.textFile(landingFile) //load actual file to RDD
        logger.info("Checking file record count..")
      } catch {
        case e: Exception => {
          fileExistsFlag = false
          landToRawFailReason = "failed while reading file. " + e.getMessage
          e.printStackTrace()
        }
      }
      try {
        fileNamePattern = getFilePattern(dataSet, vendor, feedName)
      } catch {
        case e: Exception => {
          landToRawFailReason = "Could not get file pattern from ref. file. " + e.getMessage
          e.printStackTrace()
        }
      }

      //get the file extension from ref file, fileNamePattern. eg: input -> 'PTD|SHA|PTD_DAW_CODE|abv_imm_daw_codes_999999.txt' output -> 'txt'
      fileExtensionRef = fileNamePattern.split("\\|").reverse(0).split("\\.")(1)
      //get the file name from ref file, fileNamePattern. eg: input -> 'PTD|SHA|PTD_DAW_CODE|abv_imm_daw_codes_999999.txt' output -> 'abv_imm_daw_codes_999999'
      fileNameRef = fileNamePattern.split("\\|").reverse(0).split("\\.")(0)
      //input -> abv_imm_daw_codes_999999 output -> array split by "_"
      fileNameRefArr = fileNameRef.split("\\_")

      var txn_month_patt = fileNameRefArr.reverse(0)
      var date_month_patt = fileNameRefArr.reverse(1)
      //below block checks if only date month has come in the file name
      if (Try(date_month_patt.toDouble).isSuccess == false) {
        date_month_patt = txn_month_patt
        txn_month_patt = ""
      }

      var datePatt: Regex = null
      if (txn_month_patt == "") {
        //regex for date pattern
        datePatt = "[A-z]_[0-9]{5,14}\\.txt".r

        //input -> array split by "_" output -> abv_imm_daw_codes_  
        partFileNameRef = fileNameRefArr.dropRight(1).mkString("_") + "_"

      } else {
        //regex date pattern
        datePatt = "[A-z]_[0-9]{5,8}_[0-9]{5,8}\\.txt".r

        //input -> array split by "_" output -> abv_imm_daw_codes_
        partFileNameRef = fileNameRefArr.dropRight(2).mkString("_") + "_"

      }

      val namePatt: Pattern = Pattern.compile(partFileNameRef)
      val nameMatchFlag: Matcher = namePatt.matcher(noGZExtnFilename)
      var filenamePattFlag = "Y"
      if ((datePatt.findFirstMatchIn(noGZExtnFilename).isEmpty) || (!nameMatchFlag.find())) {
        filenamePattFlag = "N"
      }

      var returnString: String = ""
      if (fileExistsFlag == true) {
        if ((recordCount == RecCountReceived) && (filenamePattFlag == "Y")) {
          goodToLoadFlag = true
          goodToLoadFlagArray += true
          returnString = "|" + dataSet + "|" + vendor + "|success|G|Y|validation successful|" + recordCount + "|" + ingestionDate
        } else if ((recordCount != RecCountReceived) && (filenamePattFlag != "Y")) {
          returnString = "|" + dataSet + "|" + vendor + "|success|B|N|file name pattern & count mismatch|" + recordCount + "|" + ingestionDate
          goodToLoadFlagArray += false
        } else if (filenamePattFlag != "Y") {
          returnString = "|" + dataSet + "|" + vendor + "|success|B|N|file name pattern mismatch|" + recordCount + "|" + ingestionDate
          goodToLoadFlagArray += false
        } else if (recordCount != RecCountReceived) {
          returnString = "|" + dataSet + "|" + vendor + "|success|B|N|count mismatch|" + recordCount + "|" + ingestionDate
          goodToLoadFlagArray += false
        }
      } else {
        landToRawFailReason = "failed to copy, as file is not present in Landing"
        returnString = "|" + dataSet + "|" + vendor + "|failure|N|N|file doesn't exist|" + recordCount + "|" + ingestionDate
        goodToLoadFlagArray += false
      }

      //append audit file info to list file line.
      if (controlFileArray(index1).contains(".zip")) {
        auditFileInfo.+=(batchID + "|" + controlFileArray(index1).replace(".zip", ".txt").replace(splitString, "|") + returnString)
      } else {
        auditFileInfo.+=(batchID + "|" + controlFileArray(index1).replace(splitString, "|") + returnString)
      }

    }

    if (FeedTypeIdentifierDIAGNOSIS == 1 && FeedTypeIdentifierACTIVITY == 1 && FeedTypeIdentifierPROCEDURE == 1) {
      feedLoadType = "F"
    }

    //save the audit file info buffer to the audit path auditfileOutputPath
    val Market_CD = readProp.getProperty(CommonConstants.MARKETCD)
    auditfileOutputPath = auditfileOutputPath.replace("DetailedAudit", "filevalidationMaster") + "/marketcd=" + Market_CD.toLowerCase() + "/load_date=" + ingestionDateAudit
    metaData = metaData + "," + ingestionDateAudit + "," + feedLoadType + "," + dataSet

    //writing batchID and load date as meta data. to be read by RAW,DQ,Refind for audit file updation
    val os = fs.create(new Path(metaDataLoc + "/loadmetadata.txt"))
    os.writeBytes(metaData)
    try {
      val tempPath=metaDataLoc.split("/").reverse(0)
      val auditfileOutputPathTemp=auditfileOutputPath+"/"+tempPath
      sc.parallelize(auditFileInfo, 1).saveAsTextFile(auditfileOutputPathTemp)
      fs.rename(new Path(auditfileOutputPathTemp + "/part-00000"), new Path(auditfileOutputPathTemp + "/filevalidation-00000"))
      
      val auditFileToMove = auditfileOutputPathTemp + "/filevalidation-00000"
      Seq("hdfs", "dfs", "-mv", auditFileToMove, auditfileOutputPath).!
      fs.delete(new Path(auditfileOutputPathTemp), true)
      spark.sql(readProp.getProperty(CommonConstants.DetailedFileLocation))
      spark.sql(readProp.getProperty(CommonConstants.DetailedAuditDB))
      spark.sql(readProp.getProperty(CommonConstants.DetailedAuditTable))

      //hiveContext.sql(readProp.getProperty(CommonConstants.DTLAUDITTBLDB))
      spark.sql(readProp.getProperty(CommonConstants.CREATEDETAILAUDITTABLE))
      spark.sql(readProp.getProperty(CommonConstants.DTLMSCK))

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    if (goodToLoadFlagArray.contains(false)) {
      logger.error("The File Validation failed and files are not good to be loaded!!!")
      //throw new Exception("The File Validation failed and files are not good to be loaded!!!")
      System.exit(1)
      // stopSparkContext()
    }
  }
}
