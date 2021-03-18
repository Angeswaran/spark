package com.humira.dataingestion

import com.humira.constants.{ CommonConstants, EnvConstants }
import com.humira.utilities.CustomLogger
import java.util.Properties
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import org.apache.hadoop.fs.Path
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import scala.sys.process._
import com.esotericsoftware.minlog.Log.Logger
//import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
//import com.humira.dataingestion.StringAccumulator
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hive.ql.exec.UDF
import java.security.MessageDigest
//import akka.io.Tcp.Write
import scala.util.control.Exception._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import java.util.Calendar
import java.time.Period
import java.time.LocalDate
import org.apache.spark.sql.types._
import sys.process._
import org.apache.spark.sql.SparkSession

/**
 * *****************************************************************************************
 *  @Author By CTS BIGDATA TEAM
 *  @Version 2.10.5
 *  @Description: Following steps are included in this class:
 *
 *         1. This script is used to move the data from Landing Layer to Raw layer.
 * 2. File un-compression: GZ/ZIP compressed files are uncompressed in the lading path folder.
 * 3. Full Load handling: If load flag from control file is “F” (full load) for a feed then,
 * it would take backup of original Raw and Refined data dir location and rename it to
 * <feed_name>_P1 and <feed_name>_P2. Max three months data is being backed-up.
 * 4. Lineage column addition: It adds lineage columns in the end of a row in data.
 * First two columns (source code, logical source code) are from reference file 'lineage-column.txt'.
 * Third column is ingestion time (date and time).
 * Fourth column (optional) is active_flag, which is added only if load type is P (Partial refresh)
 * 5. Header row removal: Header row will be removed from the Date sets which contain headers.
 * 6. Conversion to parquet format: Finally data is converted to parquet format and placed in Raw path.
 *
 * **************************************************************************************
 */

class LandingToRaw {

  def readPropeties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  var timeInSecs: Long = System.nanoTime()
  //var hiveContext: HiveContext = null
  var recordCount = ""
  var inputDirPath = ""
  var lineageColRDD: RDD[String] = null
  var fileNamePatternRDD: RDD[String] = null
  val ingestionDate = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new java.util.Date())
  val ingestionDate2 = new java.text.SimpleDateFormat("dd-MM-yyyy").format(new java.util.Date())
  var returnString: String = ""
  var fileSize: String = ""
  val auditFileInfo = collection.mutable.ArrayBuffer[String]()
  val partitionMetaInfo = collection.mutable.ArrayBuffer[String]()
  var pseudoIDMapArr = collection.mutable.ArrayBuffer[String]()
  var feedName = ""
  var lineageFilepath = ""
  var fileCheckFlag = ""
  var receivedRecordCount = ""
  var receivedFileSize = ""
  var textSchema = ""
  //var RecCountReceived = ""
  //var fileSizeReceived = ""
  var loadType = ""
  var hdfsOutputDirBasePath = ""
  var auditfileOutputPath = ""
  var lineageColAddFlag = ""
  var lineageColStr = ""
  var landToRawFailReason1 = ""
  var alreadyExecuted = false
  var lineageColsStr = ""
  var masterSchemaCols = ""
  var fileNamePattDF: DataFrame = null
  var readProp: Properties = null
  var readComProp: Properties = null
  var alreadyFilePattChk: Boolean = false
  var fileNameRefArr: Array[String] = null
  var partFileNameRef = ""
  var fileNameRef = ""
  var fileExtensionRef = ""
  var fileNamePattern = ""
  var fileNameRefLen: Int = 0
  var fileExtensionCurr = ""
  var fileNameCurr = ""
  var fileNameCurrArr: Array[String] = null
  var fileNameCurrLen = 0
  var partFileNameCurr = ""
  var fileNamePattern1 = ""
  var lineageSuccessFlag = ""
  var exceptionEncountered: Boolean = false
  var emptyFilesListArr = ArrayBuffer[String]()
  var splitString = ""
  var removedElementPosition = ArrayBuffer[Int]()


  //creating the logger instance 
  val logger = new CustomLogger().getLogger(this.getClass.toString())

  // Set Spark and Hive Configurations
  val spark = SparkSession.builder().appName("LandingToRaw2")
              .config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict")
              .config("spark.sql.parquet.compression.codec", "snappy")
              .getOrCreate()
  import spark.implicits._
  var sparkContext = spark.sparkContext
  //var sparkConf = new SparkConf().setAppName("LandingToRaw2")
  //var sparkContext = new SparkContext(sparkConf)
  //var hc = new HiveContext(sparkContext)
  //hc.setConf("hive.exec.dynamic.partition", "true")
  //hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  val conf = sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)
  //val sqlContext = spark.sqlContext
  //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
  //import sqlContext.implicits._
  /*
  def setConfigurations() {
    hiveContext = new HiveContext(sparkContext)
  }
  * 
  */

  //Stop Spark Context
  def stopSparkContext() {
    sparkContext.stop()
  }

  /*below method is created to avoid "TaskNotSerializable" exception. As it needs global var 'feedName'
  takes 2 inpts.. It returns lineage string to be appended in RDD, based on FEED value in reverse.
  ex: logical-src-cd-diagnosis|src-cd-dignosis*/
  def getlineageFunc(input1: String, input2: String): RDD[String] = {
    val lineageRDD = sparkContext.textFile(input1) //load lineage file to RDD

    try {
      //checks if 2nd column in lineage file contails the feed name, then selects that record and take last 2 cols.
      lineageColRDD = lineageRDD.filter { x => x.split("\\|")(1).contains(input2) }.map { x => x.split("\\|").reverse.take(2).mkString("|") }
    } catch {
      case e: Exception =>
        {
          e.printStackTrace()
        }

    }
    lineageColRDD
  }

  //below method is created to avoid "TaskNotSerializable" exception. As it needs var lineageColStr
  //def addLineageCol(input1: String, input2: String): RDD[String] = {
  /*def addLineageCol(input1: String, input2: RDD[String]): RDD[String] = {
    val lineageTextFileRDD = input2.filter { x => !x.isEmpty() }.map { x => x.concat("|" + input1) }
    lineageTextFileRDD
  }*/

  def addLineageCol(delimeter: String, input1: String, input2: RDD[String]): RDD[String] = {
    var line = ""
    //.filter { x => !x.isEmpty() }
    val lineageTextFileRDD = input2.map { x =>
      line = x
      if (!x.isEmpty()) {
        line = x.concat(delimeter + input1)
      }
      line
    }
    lineageTextFileRDD
  }

  def addLineageColForEnc(delimeter: String, input1: String, input2: RDD[String]): RDD[String] = {
    var line = ""
    val lineageTextFileRDD = input2.filter { x => !x.isEmpty() }.map { x =>
      line = x
      if (!x.isEmpty()) {
        line = x.concat(delimeter + input1)
      }
      line
    }
    lineageTextFileRDD
  }

  def addValuesForRemovedCol(delimeter: String, input1: String, input2: ArrayBuffer[Int], input3: RDD[String]): RDD[String] = {
    var line = ""
    val updatedTextFileRDD = input3.filter { x => !x.isEmpty() }.map { x =>
      line = x
      if (!x.isEmpty()) {
        val rowData = x.split(input1, -1)
        var rowDataUpdated = ArrayBuffer[String]()
        for (rowColsEle <- 0 to rowData.length - 1) {
          if (input2.contains(rowColsEle)) {
            rowDataUpdated += null
            rowDataUpdated += rowData(rowColsEle)
          } else {
            rowDataUpdated += rowData(rowColsEle)
          }
        }
        line = rowDataUpdated.mkString(delimeter)
      }
      line
    }
    updatedTextFileRDD
  }

  def getFilePattern(input1: String, input2: String, input3: String): String = {

    //step 1- filters lines having combination of "dataSet", "vendor", "feedName". step 2- takes the record and using mkString converts to string containing only recs whcih satisfies the previous condition.
    val fileNamePatternRecRDD = fileNamePatternRDD.filter { x => x.split("\\|")(0).equals(input1) && x.split("\\|")(1).equals(input2) && x.split("\\|")(2).equals(input3) } //.take(1).mkString("|")
    fileNamePattern1 = fileNamePatternRecRDD.take(1).mkString("|")
    fileNamePattern1
  }

  // Sub Driver main method for file format conversion
  def main1(arg: Array[String]) {
    //val stringAccum = sparkContext.accumulator("")(StringAccumulator)
    System.out.println("******************************************************** LandingToRaw4")

    readProp = readPropeties(arg(1))
    readComProp = readPropeties(arg(2))

    var inSchemePath = readProp.getProperty(CommonConstants.INCOMINGSCHEMAPATH) + "/" + readComProp.getProperty(CommonConstants.INCOMINGSCHEMAFILE)

    val masterSchemaArr = sparkContext.textFile(inSchemePath).take(1)(0).split("\\|")
    var masterSchemaColsArr = ArrayBuffer[String]()

    //extract only column names from it, example : MARKET_CODE,HH_INCOME_CODE,HH_INCOME_DESC,DATE_MONTH
    for (ele <- 0 to masterSchemaArr.length - 1) {
      masterSchemaColsArr += masterSchemaArr(ele).split(" ")(0)
    }
    var schemaElements = masterSchemaColsArr.length
    masterSchemaCols = masterSchemaColsArr.mkString(",")
    var removedColumnArray = ArrayBuffer[String]()
    val schemaChangeTrackerPath = readProp.getProperty(EnvConstants.SCHEMACHANGETRACKERPATH) + "/" + readComProp.getProperty(CommonConstants.MASTERSCHEMAFILENAME)
    val schemaRemovalTrackerPath = readProp.getProperty(EnvConstants.SCHEMAREMOVALTRACKERPATH) + "/" + readComProp.getProperty(CommonConstants.MASTERSCHEMAFILENAME)
    var containsMultiLine = readProp.getProperty(EnvConstants.CONTAINSMULTILINE)
    //logger.info("Setting Configurations..")
    //setConfigurations()

    val listFilePath = readProp.getProperty(CommonConstants.FILELISTCURRENTABSOLUTEPATH) + "/" + readComProp.getProperty(CommonConstants.FILELISTNAME)
    inputDirPath = readProp.getProperty(CommonConstants.LANDINGBASEPATH)
    lineageFilepath = readProp.getProperty(CommonConstants.LINEAGEFILEABSOLUTEPATH)
    val hdfsOutputDirBasePath = readProp.getProperty(EnvConstants.RAWBASEPATH)
    var auditfileOutputPath = readProp.getProperty(CommonConstants.AUDITFILEBASEPATH)
    val RefinedBasePathStr = readProp.getProperty(EnvConstants.REFINEDBASEPATH)
    val metaDataLoc = readProp.getProperty(CommonConstants.METADATALOCATION)
    val feedFrmPropFile = readComProp.getProperty(CommonConstants.FEEDFROMPROPFILE).toUpperCase()
    val dataSet = readProp.getProperty(CommonConstants.DATASET)
    val vendor = readProp.getProperty(CommonConstants.VENDOR)
    val data_Set = vendor.toUpperCase() + "_" + dataSet.toUpperCase()
    val p1p2CreateFlag = readComProp.getProperty(CommonConstants.P1P2CREATEFLAG)
    var date_month = ""
    var pLoadType = "NA"
    var partialFrmDate = ""
    var partialToDate = ""
    splitString = readComProp.getProperty(CommonConstants.RAWTBLDELIMITER)
    val delimeter = readComProp.getProperty(CommonConstants.RAWTBLDELIMITER)
    splitString = "\\" + splitString
    val isStringQualifierDoubleQuotes = readComProp.getProperty(CommonConstants.ISSTRINGQUALIFIERDOUBLEQUOTES)
    
    if(!(isStringQualifierDoubleQuotes.equalsIgnoreCase("Y"))){
     splitString = splitString + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    }
  
    val splitStringLocal = splitString

    var removeHeader = readComProp.getProperty(CommonConstants.REMOVEHEADER)
    if (removeHeader == null) {
      removeHeader = "false"
    }
    logger.info("Remove Header = " + removeHeader)

    val partialRefFlag = readComProp.getProperty(CommonConstants.PARTIALREFFLAG).toUpperCase()

    //extract load date from the meta data file
    val loadDate = sparkContext.textFile(metaDataLoc + "/loadmetadata.txt").take(1)(0).split("\\,")(1)

    //Get audit file
    val Market_CD = readProp.getProperty(CommonConstants.MARKETCD)
    val auditFileAbsPath = auditfileOutputPath + "/marketcd=" + Market_CD.toLowerCase() + "/load_date=" + loadDate
    val auditFileMasterPath = auditfileOutputPath.replace("DetailedAudit", "filevalidationMaster") + "/marketcd=" + Market_CD.toLowerCase() + "/load_date=" + loadDate
    var readAuditFile: RDD[String] = null

    try {
      readAuditFile = sparkContext.textFile(auditFileMasterPath).cache()
    } catch {
      case e: Exception =>
        {
          e.printStackTrace()
        }

    }

    lineageSuccessFlag = "Y"
    try {
      lineageColStr = getlineageFunc(lineageFilepath, feedFrmPropFile).take(1)(0).split("\\|").reverse.mkString(delimeter)
      lineageColStr = lineageColStr + delimeter + ingestionDate
    } catch {
      case e: Exception =>
        {
          lineageSuccessFlag = "N"
          landToRawFailReason1 = "failed while getting lineage cols from lineage col file. " + e.getMessage
          e.printStackTrace()
        }

    }

    //add lineage header in schema string. data sets with partial refresh will have an extra col - active_flag.

    //get the control file, with all feed and file lists to be processed
    val listFileRDD = sparkContext.textFile(listFilePath)
    val listFileArray = listFileRDD.collect() //collect RDD into an Array

    var FullLoadExists = ""

    FullLoadExists = listFileArray(0).split(splitString)(4)
    loadType = listFileArray(0).split(splitString)(4)
    feedName = listFileArray(0).split(splitString)(3).split(" ")(0)

    if (FullLoadExists == "F") {
      logger.info("----Load type is F ..")
      val incomingSchemaFile = readComProp.getProperty(CommonConstants.RAWNEWSCHEMALOCATION)
      val feednameFronSchemaFile = readComProp.getProperty(CommonConstants.MASTERSCHEMAFILENAME).replace(".txt", "")
      val infilePath = readProp.getProperty(EnvConstants.STRINGCHANGEDSCHEMAPATH) + "/" + feednameFronSchemaFile + "/" + incomingSchemaFile
      val inFile = new Path(infilePath)
      /*try {
        if (fs.exists(inFile)) {
          val schemaChangeTrackerFile = fs.create(new Path(schemaChangeTrackerPath), true)
          schemaChangeTrackerFile.writeBytes("")
          val schemaRemovalTrackerFile = fs.create(new Path(schemaRemovalTrackerPath), true)
          schemaRemovalTrackerFile.writeBytes("")
        }
      } catch {
        case e: Exception =>
          {
          }

      }*/
      //RAW -  Change pointers
      //Step 1: remove the _P2 (prior 2) path
      if (p1p2CreateFlag.equalsIgnoreCase("Y"))
      {
      try {
        fs.delete(new Path(hdfsOutputDirBasePath + "/" + feedName + "_P2"), true)
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not delete " + hdfsOutputDirBasePath + "/" + feedName + "_P2 " + e.getMessage
            logger.error("Could not delete " + hdfsOutputDirBasePath + "/" + feedName + "_P2 " + e.getMessage)
            e.printStackTrace()
          }

      }
      //Step 2: re-name _P1 (prior 1) to _P2
      try {
        fs.rename(new Path(hdfsOutputDirBasePath + "/" + feedName + "_P1"), new Path(hdfsOutputDirBasePath + "/" + feedName + "_P2"))
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not rename " + hdfsOutputDirBasePath + "/" + feedName + "_P1 " + e.getMessage
            logger.error("Could not rename " + hdfsOutputDirBasePath + "/" + feedName + "_P1 " + e.getMessage)
            e.printStackTrace()
          }

      }
      //Step 3: re-name original dir to _P1
      try {
        fs.rename(new Path(hdfsOutputDirBasePath + "/" + feedName), new Path(hdfsOutputDirBasePath + "/" + feedName + "_P1"))
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not rename " + hdfsOutputDirBasePath + "/" + feedName + e.getMessage
            logger.error("Could not rename " + hdfsOutputDirBasePath + "/" + feedName + e.getMessage)
            e.printStackTrace()
          }

      }
      }
            if (p1p2CreateFlag.equalsIgnoreCase("N"))
            {
      //Step 4: remove original dir
      try {
        fs.delete(new Path(hdfsOutputDirBasePath + "/" + feedName), true)
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not delete " + hdfsOutputDirBasePath + "/" + feedName + e.getMessage
            logger.error("Could not delete " + hdfsOutputDirBasePath + "/" + feedName + e.getMessage)
            e.printStackTrace()
          }
      }
            }

    }
    val removalSchemardd = sparkContext.textFile(schemaRemovalTrackerPath)
    if (removalSchemardd.count() > 0) {
      val removedColumn = removalSchemardd.take(1)(0)
      val removedColumnArr = removedColumn.split("\\|")
      if (removedColumnArr.length > 0) {
        //extract only column names from it
        for (ele <- 0 to removedColumnArr.length - 1) {
          removedColumnArray += removedColumnArr(ele).split(" ")(0)
        }
        removedColumnArray = removedColumnArray.distinct
      }
    }
    var counterForArrayReduction = 0
    for (removedele <- removedColumnArray) {
      if (masterSchemaColsArr.contains(removedele)) {
        removedElementPosition += masterSchemaColsArr.indexOf(removedele) - counterForArrayReduction
      }
      counterForArrayReduction = counterForArrayReduction + 1
    }
    var encrptyIndivIDExists = false
    var encrptyPatientIDExists = false

    for (colmnValue <- listFileArray.par) {

      val index1 = listFileArray.indexOf(colmnValue)

      var outputDirPathTemp = ""
      var outputDirPath = ""

      var lineageSuccessFlag1 = "Y"
      var copySuccessFlag = "failure"
      var timeInSecs = System.nanoTime()
      var landToRawFailReason = ""

      // get all the required info from list file
      val fileInfoArray = listFileArray(index1).split(splitString)

      val fileNameArray = fileInfoArray(0).split("\\.")
      val gzFileName = fileInfoArray(0)

      var fileName = ""

      if (gzFileName.endsWith("gz")) {
        //Uncompress .gz files
        logger.info("----Uncompressing file " + gzFileName)
        fileName = fileInfoArray(0).split("\\.").dropRight(1).mkString(".") //remove .gz from the file name
        logger.info("file name = " + fileName)

        try {
          Seq("hdfs", "dfs", "-cat", inputDirPath + "/" + gzFileName) #| Seq("gzip", "-d") #| Seq("hdfs", "dfs", "-put", "-", inputDirPath + "/" + fileName) !
        } catch {
          case e: Exception =>
            {
              e.printStackTrace()
            }
        }
      } else {
        val fileFormat = fileInfoArray(0).split("\\.").reverse(0)
        if (fileFormat.equals("csv")) {
          fileName = fileInfoArray(0).split("\\.").dropRight(1).mkString(".") + ".csv" //remove .gz from the file name
        } else if (fileFormat.equals("tsv")){
          fileName = fileInfoArray(0).split("\\.").dropRight(1).mkString(".") + ".tsv" //remove .gz from the file name
        }
        else {
          fileName = fileInfoArray(0).split("\\.").dropRight(1).mkString(".") + ".txt" //remove .gz from the file name
        }
        logger.info("file name = " + fileName)
      }

      var txn_month = fileNameArray(0).split("\\_").reverse(0)
      date_month = fileNameArray(0).split("\\_").reverse(1)

      //below block checks if only date month has come in the file name
      if (Try(date_month.toDouble).isSuccess == false) {
        date_month = txn_month
        txn_month = ""
      }
      //val RecCountReceived = fileInfoArray(1)
      //val fileSizeReceived = fileInfoArray(2)

      logger.info("Loading file from input path " + inputDirPath + "..")
      var inputDirPathFile = ""
      inputDirPathFile = inputDirPath + "/" + fileName

      var copyFileFlag = "N"
      var textFileRDD = sparkContext.textFile(inputDirPathFile) //load actual file to RDD
      var textFileRDDUpdated: RDD[String] = null
      var textFileRDDUpdatedColumnAdded: RDD[String] = null
      if (removeHeader == "true") {
        var numOfHeaderLines = readComProp.getProperty(CommonConstants.NUMBEROFHEADER)
        val numOfHeaderLinesInt = numOfHeaderLines.toInt
        textFileRDDUpdatedColumnAdded = textFileRDD
        for (n <- 0 to (numOfHeaderLinesInt - 1)) {
          val header = textFileRDDUpdatedColumnAdded.first() //extract header
          textFileRDDUpdatedColumnAdded = textFileRDDUpdatedColumnAdded.filter(r => r != header) //filter out header
        }
      } else {
        textFileRDDUpdatedColumnAdded = textFileRDD
      }
      if (!textFileRDDUpdatedColumnAdded.isEmpty()) {
        textFileRDDUpdated = addValuesForRemovedCol(delimeter, splitString, removedElementPosition, textFileRDDUpdatedColumnAdded)
        val dataSchemaArray = textFileRDDUpdated.take(1)(0).split(splitString)
        val dataSchemaCount = dataSchemaArray.length
        if (schemaElements != dataSchemaCount) {
          System.out.println("SchemaChanged=1")
          //* Dynamic Schema Handling script *//
          /*
        var colDiff = dataSchemaCount - schemaElements
        if (colDiff > 0) {
          var new_columns = ""
          for (colCount <- 1 to colDiff) {
            masterSchemaCols = masterSchemaCols + ",NewCol" + colCount
            new_columns = new_columns + ",NewCol" + colCount + " string"
          }
          new_columns = new_columns.substring(1)
          val alter_query = readComProp.getProperty(CommonConstants.ALTERTABLE_RAW) + "(" + new_columns + ")"

          hc.sql(alter_query)
        }
        */
        } else {
          System.out.println("SchemaChanged=0")
        }

        if (partialRefFlag == "Y") {
          textSchema = masterSchemaCols + ",src_code,logical_src_code,raw_ingestion_time,active_flag"
          lineageColStr = lineageColStr + delimeter + "Y"
          lineageColsStr = lineageColStr

        } else {
          textSchema = masterSchemaCols + ",src_code,logical_src_code,raw_ingestion_time"
          lineageColsStr = lineageColStr
        }
        val isColumnReplaced = readProp.getProperty(EnvConstants.ISCOLUMNREPLACED)
        val isDatalakeID = readProp.getProperty(EnvConstants.ISDATALAKEID)
        var replacementValue = ""

        val textSchemaBeforeReplaced = textSchema
        if (isColumnReplaced.equalsIgnoreCase("true")) {
          val columnToReplace = readComProp.getProperty(CommonConstants.COLUMNTOREPLACE)
          replacementValue = readComProp.getProperty(CommonConstants.REPLACEMENTVALUE)
          textSchema = textSchema.replace(columnToReplace, replacementValue)
        }
        var schemaForMapping = textSchema.split(",")

        //get record count of loaded file and catch the exception, if file not found and set flag.
        var recordCountArr: Array[String] = null
        var recordCount = ""
        var fileExistsFlag: Boolean = true

        //create output dir path. create txn_month dir only if it has come in file name
        if (txn_month != "") {
          outputDirPathTemp = hdfsOutputDirBasePath + "/" + feedName + "/date_month=" + date_month + "/txn_month=" + txn_month + "/"
        } else {
          outputDirPathTemp = hdfsOutputDirBasePath + "/" + feedName + "/date_month=" + date_month + "/"
        }
        val currentimestamp: Long = System.currentTimeMillis / 1000
        var lineageTextFileFinRDD: RDD[String] = null
        try {
          logger.info("Adding Lineage columns for Feed " + feedName + "..")

          if (readProp.getProperty(EnvConstants.ENCRYPTIONFLAG).toUpperCase == "Y") {
            lineageTextFileFinRDD = addLineageColForEnc(delimeter, lineageColsStr, textFileRDDUpdated)
          } else {
            lineageTextFileFinRDD = addLineageCol(delimeter, lineageColsStr, textFileRDDUpdated)
          }
        } catch {
          case e: Exception =>
            {
              lineageSuccessFlag1 = "N"
              landToRawFailReason = "failed while adding lineage cols to file data. " + e.getMessage
              e.printStackTrace()
            }

        }

        //do below only if lineage column extraction from lineage file is successful
        if ((lineageSuccessFlag == "Y") && (lineageSuccessFlag1 == "Y")) {

          //below code block will convert RDD[String] to DataFrame
          val textSchemaStruct = StructType(schemaForMapping.map { fieldName => StructField(fieldName, StringType, true) })
          var textRowRDD: RDD[Row] = null
          if (readProp.getProperty(EnvConstants.REMOVEQUOTES) == "true") {
            val regexString = delimeter + "(?=[^\"]*\"[^\"]*(?:\"[^\"]*\"[^\"]*)*$)"
            val regexObj = regexString.r
            lineageTextFileFinRDD = lineageTextFileFinRDD.map { x => regexObj.replaceAllIn(x, " ") }
            textRowRDD = lineageTextFileFinRDD.map(_.split(splitStringLocal, -1).map(_.trim.replace("\"", ""))).map(x => x.toSeq).map { x => Row.fromSeq(x) }
          } else {
            textRowRDD = lineageTextFileFinRDD.map(_.split(splitStringLocal, -1)).map(x => x.toSeq).map { x => Row.fromSeq(x) }
          }
          var textDF = spark.createDataFrame(textRowRDD, textSchemaStruct)
          val nullConverter = udf((input: String) => {
            if (input.trim.length > 0) input.trim
            else null
          })
          textDF = textDF.select(textDF.columns.map(c => nullConverter(col(c)).alias(c)): _*)
           
          

          var masterSchemaColsArr2 = ArrayBuffer[String]()
          val changedSchemardd = sparkContext.textFile(schemaChangeTrackerPath)
          if (changedSchemardd.count() > 0) {
            val schemaD = changedSchemardd.take(1)(0)
            val masterSchemaArr2 = schemaD.split("\\|")
            if (masterSchemaArr2.length > 0) {
              //extract only column names from it
              for (ele <- 0 to masterSchemaArr2.length - 1) {
                masterSchemaColsArr2 += masterSchemaArr2(ele).split(" ")(0)
              }
            }
          }

          // ----------------- PARTIAL LOGIC IMPLEMENTATION

          /* If load type is P, do following for implementing partial refresh in data:
         * 1. Fetching partialFrmDate and partialToDate from the .lst file.
         * 2. For the dates between partialFrmDate and partialToDate, hive query is written to retrieve date_month column.
         * 3. Will load that partition in Dataframe.
         * 4. For each partition will do following:
         *                    a. Will go to HDFS dir path for that particular partition.
         *                    b. Read all parquet files.
         *                    c. Fetch transaction_date column index 
         *                    d. Loading the files from the affected partition to DF.
         *                    e. Converting dates to desired format for comparison. Used UDF as well.
         *                    f. If transaction_date lies between Refresh_From_Date and Refresh_To_Date, updating active_flag from "Y" to "N" in DataFrame 
         *                    g. Clean the affected partition path (Delete all files)
         *                    h. Move all .parquet files from temp folder to affected partition
         *                    i. Lastly delete temp folder
         */
          partialFrmDate = ""
          partialToDate = ""
          var rawAffectedPartPath = ""

          val rawDB = readProp.getProperty(EnvConstants.DATABASENAME1)
          val rawTableName = readComProp.getProperty(CommonConstants.RAWTBLNAME).replace("SET hivevar:stgtable=", "")
          val transactionDateColName = readComProp.getProperty(CommonConstants.TRANSACTIONDATECOLNAME)
          val transactionDateFormat = readComProp.getProperty(CommonConstants.TRANSACTIONDATEFORMAT)

          if (loadType.trim() == "P") {
            pLoadType = "P"
val oldfeedmetadata = spark.read.textFile(readProp.getProperty(CommonConstants.METADATALOCATION) + "/" + feedName + "_feedmetadata.txt").take(1).mkString(",")

        if (oldfeedmetadata.isEmpty || oldfeedmetadata == null) {
          logger.error("No lines found in the feedmetadata file ..")
          throw new RuntimeException("No lines found in the feedmetadata file ..")
        }
        val feedMetadataArr = oldfeedmetadata.split(',')
        // val date_month = feedMetadataArr(0)
        val old_date_month = feedMetadataArr(0) 
            partialFrmDate = fileInfoArray(5) //partial refresh "from date"
            partialToDate = fileInfoArray(6) //partial refresh "to date"

            // Fetching affected Partition
            val rawPartitionCol = "select distinct date_month from " + rawDB + "." + rawTableName + " where " + "TO_DATE(from_unixtime(unix_timestamp(trim(" + transactionDateColName + ") , '" + transactionDateFormat + "'))) >= TO_DATE(from_unixtime(unix_timestamp('" + partialFrmDate + "' , '" + transactionDateFormat + "'))) and TO_DATE(from_unixtime(unix_timestamp(trim(" + transactionDateColName + ") , '" + transactionDateFormat + "'))) <= TO_DATE(from_unixtime(unix_timestamp('" + partialToDate + "' , '" + transactionDateFormat + "')))" + "and date_month='"+old_date_month.split('=')(1)+"'" 

            //val hqlCon = new HiveContext(sparkContext)
            val rawAffectedPartColDF = spark.sql(rawPartitionCol)

            // Looping all partitions, in case of multiple partition
            rawAffectedPartColDF.collect().foreach { x =>
              rawAffectedPartPath = hdfsOutputDirBasePath + "/" + feedName + "/date_month=" + x(0) + "/"

              // Loading the files from the affected partition to DF
              val rawAffectedPartDF = spark.read.parquet(rawAffectedPartPath)

              // Updating active_flag from "Y" to "N" in DataFrame; if transaction_date lies between Refresh_From_Date and Refresh_To_Date
              /*val rawUpdatedDF = rawAffectedPartDF.withColumn(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME), 
                when(rawAffectedPartDF(transactionDateColName).isNotNull and (rawAffectedPartDF(transactionDateColName) !== "") 
                and dateFormatterUDF(rawAffectedPartDF(transactionDateColName)) >= partialFrmDateValue 
                and dateFormatterUDF(rawAffectedPartDF(transactionDateColName)) <= partialToDateValue, "N")
                .otherwise("Y"))*/
              //changing output date format to yyyyMM if input date format is yyyyMM, as it does not have dd
              var rawUpdatedDF: DataFrame = null
              if (transactionDateFormat.contains("MMM")) {
                // Converting date format of Refresh_From_Date and Refresh_To_Date to desired format(yyyyMMdd), for comparison
                val inptDateFmt: SimpleDateFormat = new SimpleDateFormat(transactionDateFormat)
                var outputDateFmt = new SimpleDateFormat("yyyyMMdd")

                if (transactionDateFormat.contains("HH:mm:ss")) {
                  outputDateFmt = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
                }

                val partialFrmDateParsed = inptDateFmt.parse(partialFrmDate)
                val partialFrmDateValue = outputDateFmt.format(partialFrmDateParsed)
                val partialToDateParsed = inptDateFmt.parse(partialToDate)
                val partialToDateValue = outputDateFmt.format(partialToDateParsed)

                //UDF - to convert transaction_date to desired format(yyyyMMdd)
                val dateFormatterUDF = udf[String, String] { (inCol1: String) =>
                  var inCol1Parsed: Date = null
                  if (inCol1 != "") {
                    val inCol1Parsed = inptDateFmt.parse(inCol1)
                    outputDateFmt.format(inCol1Parsed)
                  } else {
                    ""
                  }
                }

                /*rawUpdatedDF = rawAffectedPartDF.withColumn(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
                when(dateFormatterUDF(rawAffectedPartDF(transactionDateColName)) >= partialFrmDateValue
                  and dateFormatterUDF(rawAffectedPartDF(transactionDateColName)) <= partialToDateValue, "N")
                  .otherwise("Y"))*/

                rawUpdatedDF = rawAffectedPartDF.withColumn(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
                  when(rawAffectedPartDF(transactionDateColName).isNull and (rawAffectedPartDF(transactionDateColName) === ""), "Y")
                    .otherwise(when(dateFormatterUDF(rawAffectedPartDF(transactionDateColName)) >= partialFrmDateValue
                      and dateFormatterUDF(rawAffectedPartDF(transactionDateColName)) <= partialToDateValue and (rawAffectedPartDF(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME)) === "Y"), "N").otherwise("Y"))) 
              } else {
                /*rawUpdatedDF = rawAffectedPartDF.withColumn(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
                when(rawAffectedPartDF(transactionDateColName) >= partialFrmDate
                  and rawAffectedPartDF(transactionDateColName) <= partialToDate, "N")
                  .otherwise("Y"))*/

                rawUpdatedDF = rawAffectedPartDF.withColumn(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
                  when(rawAffectedPartDF(transactionDateColName).isNull and (rawAffectedPartDF(transactionDateColName) === ""), "Y")
                    .otherwise(when(rawAffectedPartDF(transactionDateColName) >= partialFrmDate
                      and rawAffectedPartDF(transactionDateColName) <= partialToDate and (rawAffectedPartDF(readComProp.getProperty(CommonConstants.ACTIVEFLAGCOLNAME)) === "Y"), "N").otherwise("Y"))) 
              }

              val rawAffectedPartPathTemp = hdfsOutputDirBasePath + "/" + feedName + "/temp"
              val rawFilesToMove = rawAffectedPartPathTemp + "/*.parquet"
              val rawAffectedPartPathCleanUp = rawAffectedPartPath + "*"

              rawUpdatedDF = rawUpdatedDF.select(schemaForMapping.head, schemaForMapping.tail: _*)
              if (changedSchemardd.count() > 0) {
                for (changedSchema <- masterSchemaColsArr2) {
                  if (rawUpdatedDF.columns.contains(changedSchema)) {
                    val intermediateDF = rawUpdatedDF.withColumn("ChangedColumn", rawUpdatedDF(changedSchema))
                    rawUpdatedDF = intermediateDF.drop(changedSchema).withColumnRenamed("ChangedColumn", changedSchema)
                  }
                }
              }
              rawUpdatedDF.write.parquet(rawAffectedPartPathTemp)

              Seq("hdfs", "dfs", "-rm", rawAffectedPartPathCleanUp).!
              Seq("hdfs", "dfs", "-mv", rawFilesToMove, rawAffectedPartPath).!
              Seq("hdfs", "dfs", "-rm", "-r", rawAffectedPartPathTemp).!

              copySuccessFlag = "success"
            }

          }

          // ----------------------END OF PARTIAL REFRESH LOGIC

          logger.info("Writing file- " + fileName + " in Parquet format..")

          // ----------------------Start of Deidentification Process----------------------------

          // ----------------------Start of pseudo id and encryption logic  
          try {
            var textDFWithAge: DataFrame = null

            if (readProp.getProperty(EnvConstants.ENCRYPTIONFLAG).toUpperCase == "Y") {

              //-----------------------Deidentification UDF and methods--------------------------

              conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "jceks://hdfs/keystore/epsilonkey.jceks")
              val secretKeyArr = conf.getPassword("epsilonkey.alias")
              val secretKey = secretKeyArr.mkString("")
              val pseudoKey = secretKey.split("\\|")(0).toLong
              val encryptionKey = secretKey.split("\\|")(1)

              //UDF - to generate pseudo if for original id and then encrypt this pseudo id.
              var pseudoId = ""
              val pseudoIdGeneration = udf[String, String] { (inCol1: String) =>
                //pseudoId = inCol1.replace("\"", "")
                pseudoId = inCol1
                if (pseudoId != null && !pseudoId.isEmpty()) {
                  //pseudoId = MessageDigest.getInstance("MD5").digest(inCol1.getBytes).map("%02x".format(_)).mkString
                  var xorId1 = 0l
                  if (inCol1.replace("\"", "").startsWith("PID-")) {

                    val intInCol1 = allCatch.opt(inCol1.replace("\"", "").substring(4).toLong)
                    if (intInCol1 != None) {
                      xorId1 = inCol1.replace("\"", "").substring(4).toLong ^ pseudoKey
                      val complimentId = (~xorId1).abs
                      //pseudoId = "PID-" + complimentId.toString()
                      pseudoId = complimentId.toString()
                    } else {
                      //logger.error("Detected Input in bad format. Cannot process:" + inCol1)
                    }
                  } else if (inCol1.replace("\"", "").startsWith("CH-")) {
                    val intInCol1 = allCatch.opt(inCol1.replace("\"", "").substring(3).toLong)
                    if (intInCol1 != None) {
                      xorId1 = inCol1.replace("\"", "").substring(3).toLong ^ pseudoKey
                      val complimentId = (~xorId1).abs
                      //pseudoId = "PID-" + complimentId.toString()
                      pseudoId = complimentId.toString()
                    } else {
                      //logger.error("Detected Input in bad format. Cannot process:" + inCol1)
                    }
                  } else {
                    var convertedId = ""
                    for (c <- pseudoId) {
                      if (!c.isDigit) {
                        convertedId = convertedId + c.toInt
                      } else {
                        convertedId = convertedId + c
                      }
                    }
                    import BigInt._
                    var pseudoBigKey = BigInt(pseudoKey)
                    var xorBigId1 = BigInt(xorId1)
                    xorBigId1 = BigInt(convertedId) ^ pseudoBigKey
                    val complimentId = (~xorBigId1).abs
                    pseudoId = complimentId.toString()
                  }
                }
                pseudoId
              }

              var encryptedID = ""
              val EncryptionUDF = udf[String, String] { (inCol1: String) =>
                encryptedID = inCol1

                if (encryptedID != null && !encryptedID.isEmpty()) {
                  //below code is for AES encryption of pseudo Id
                  val password = new String(encryptionKey).getBytes()
                  // The password need to be 8, 16, 32 or 64 characters long to be used in AES encryption
                  val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
                  val keySpec = new SecretKeySpec(password, "AES")
                  cipher.init(Cipher.ENCRYPT_MODE, keySpec)
                  encryptedID = Base64.encodeBase64String(cipher.doFinal(inCol1.getBytes()))
                }
                encryptedID
              }

                var maskingCharacter = ""
                var startOrEnd = ""
                var numberOfDigits = "" 
              //UDF - to generate masked id
                  val maskedIdGeneration = udf[String, String] { (inCol1: String) =>
                    var maskedId = inCol1
                    if (maskedId != null && !maskedId.isEmpty()) {
                      if (startOrEnd.equalsIgnoreCase("End")) {
                        val unmaskedString = maskedId.substring(0, maskedId.length() - numberOfDigits.toInt)
                        //repeat the MaskingCharacter as many as numberOfDigits
                        val maskedString = maskingCharacter.toString * numberOfDigits.toInt
                        maskedId = unmaskedString.concat(maskedString)
                      } else {
                        //println("it is " + maskedId +"str,end : " + startOrEnd )
                        val unmaskedString = maskedId.substring(numberOfDigits.toInt) // (4 -2 , 4-1) 2,3
                        //repeat the MaskingCharacter as many as numberOfDigits
                        val maskedString = maskingCharacter.toString * numberOfDigits.toInt
                        maskedId = maskedString.concat(unmaskedString)
                      }
                    } else {
                      maskedId
                    }
                    maskedId
                  }
                  
              logger.info("ENCRYPTIONFLAG is " + readProp.getProperty(EnvConstants.ENCRYPTIONFLAG))

              var lowerAgeLimit = ""
              var lowerAgeLimitString = ""
              var upperAgeLimit = ""
              var upperAgeLimitString = ""
              var column_name = ""
              var rule_name = ""
              //var numberOfDigits = ""
              //var startOrEnd = ""
              //var maskingCharacter = ""
              var populationDF: DataFrame = null
              var textDF2: DataFrame = null
              var currentPseudoMapDF: DataFrame = null
              var currDataReplacedWithPseudoIdDF: DataFrame = null
              val deidentificationTable = readProp.getProperty(EnvConstants.DEIDENTIFICATIONTABLE)
              val ZipCodePopulationTable = readProp.getProperty(EnvConstants.ZIPCODEPOPTABLE)

              val deidentificationDF = spark.sql("select * from " + deidentificationTable + " where table_name='" + feedName.toLowerCase() + "' and active_flag='Y'")
              deidentificationDF.show
              logger.info("deidentificationDF")

              if (deidentificationDF.count() > 0) {
                var listDeIdentify = deidentificationDF.map { x => x.mkString(",") }.collect
                for (deIdentify <- listDeIdentify) {

                  var fieldscountTmp = deIdentify.split(",", -1)
                  column_name = fieldscountTmp(1).toUpperCase()
                  rule_name = fieldscountTmp(2)

                  logger.info("Deidentification Rule to be applied on data is " + rule_name)

                  if (rule_name.equalsIgnoreCase("agecalculation")) {
                    lowerAgeLimit = fieldscountTmp(3).split(":")(0)
                    lowerAgeLimitString = fieldscountTmp(3).split(":")(1)
                    upperAgeLimit = fieldscountTmp(5).split(":")(0)
                    upperAgeLimitString = fieldscountTmp(5).split(":")(1)
                    val today = Calendar.getInstance().getTime
                    val formatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    val curTime = formatted.format(today)
                    val curr_date = year(unix_timestamp(lit(curTime), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
                    val curr_date_month = month(unix_timestamp(lit(curTime), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
                    val curr_date_day = dayofmonth(unix_timestamp(lit(curTime), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
                    val DOB = year(unix_timestamp(textDF(column_name), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
                    val DOB_month = month(unix_timestamp(textDF(column_name), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
                    val DOB_day = dayofmonth(unix_timestamp(textDF(column_name), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
                    textDF = textDF.withColumn("calCulated_age", ((curr_date - DOB) + (curr_date_month - DOB_month) / 12 + (curr_date_day - DOB_day) / 365))

                    var InterDF1 = textDF.filter((textDF("calCulated_age").<(lowerAgeLimit.toDouble))).withColumn(column_name, lit(lowerAgeLimitString))

                    var InterDF2 = textDF.filter((textDF("calCulated_age").>=(lowerAgeLimit.toDouble)).and(textDF("calCulated_age").<=(upperAgeLimit.toDouble))).withColumn(column_name, floor(textDF("calCulated_age")))
                    var InterDF3 = InterDF1.unionAll(InterDF2)
                    var InterDF4 = textDF.filter((textDF("calCulated_age").>(upperAgeLimit.toDouble))).withColumn(column_name, lit(upperAgeLimitString))
                    textDF = InterDF3.unionAll(InterDF4).unionAll(textDF.filter(textDF("calCulated_age").isNull)).drop("calCulated_age")

                  }
                  if (rule_name.equalsIgnoreCase("ageMasking")) {
                    lowerAgeLimit = fieldscountTmp(3).split(":")(0)
                    lowerAgeLimitString = fieldscountTmp(3).split(":")(1)
                    upperAgeLimit = fieldscountTmp(5).split(":")(0)
                    upperAgeLimitString = fieldscountTmp(5).split(":")(1)
 
                    var InterDF1 = textDF.filter((textDF(column_name).<(lowerAgeLimit.toDouble))).withColumn(column_name, lit(lowerAgeLimitString))
 
                    var InterDF2 = textDF.filter((textDF(column_name).>=(lowerAgeLimit.toDouble)).and(textDF(column_name).<=(upperAgeLimit.toDouble))).withColumn(column_name, textDF(column_name))
                    var InterDF3 = InterDF1.unionAll(InterDF2)
                    var InterDF4 = textDF.filter((textDF(column_name).>(upperAgeLimit.toDouble))).withColumn(column_name, lit(upperAgeLimitString))
                    textDF = InterDF3.unionAll(InterDF4).unionAll(textDF.filter(textDF(column_name).isNull))
                  }

                  if (rule_name.equalsIgnoreCase("masking")) {
                    numberOfDigits = fieldscountTmp(3)
                    startOrEnd = fieldscountTmp(4)
                    maskingCharacter = fieldscountTmp(5)
                    populationDF = spark.sql("select geoid,pop10 from " + ZipCodePopulationTable)

                    val maskSeq = populationDF.filter($"pop10" <= 20000).select($"geoid").collect.toSeq.flatMap(ele => ele.toSeq)
                    val text1DF = textDF.withColumn("actual_value", textDF(column_name))
                    val textDF1_temp = text1DF.filter((textDF(column_name) isin (maskSeq: _*)))
                    val textDF1 = textDF1_temp.withColumn(column_name, maskedIdGeneration(textDF(column_name)))
                    val textDF2 = text1DF.filter(!(textDF(column_name) isin (maskSeq: _*)))
                    val textDF3 = text1DF.filter(textDF(column_name).isNull)
                    val finalunionDF = textDF1.unionAll(textDF2).unionAll(textDF3)
                    var textMaskedDF = textDF.withColumn(column_name, maskedIdGeneration(textDF(column_name)))
                    textDF = textMaskedDF
                  }

                  if (rule_name.equalsIgnoreCase("pseudonymisation")) {

                    logger.info("applying pseudonymisation logic")
                    encrptyIndivIDExists = true
                    encrptyPatientIDExists = true

                    var pseudoTextDF: DataFrame = null
                    var currentTempPseudoMapDF: DataFrame = null
                    var currDataReplacedWithPseudoIdTempDF: DataFrame = null
                    var pseudoTextTempDF: DataFrame = null
                    var idColumnsList = column_name
                    val idColumns = idColumnsList.split('|')
                    for (i <- 0 until idColumns.length) {
                      textDF = textDF.withColumn("TEMP_" + idColumns(i), textDF(idColumns(i)))
                    }
                    textDF2 = textDF
                    // println("textDF2")
                    // textDF2.show
                    for (i <- 0 until idColumns.length) {
                      println("idColumns(i) " + idColumns(i))
                      textDF2 = textDF2.withColumn(idColumns(i), pseudoIdGeneration(textDF2(idColumns(i))))
                    }
                    pseudoTextDF = textDF2
                    println("pseudoTextDF")
                    pseudoTextDF.show(2000)
                    pseudoTextDF.cache()
                    var doublestr = ""
                    for (i <- 0 until idColumns.length) {
                      //    doublestr = doublestr + idColumns(i) + ",TEMP_" + idColumns(i) + ","
                      doublestr = doublestr + "TEMP_" + idColumns(i) + "," + idColumns(i) + ","
                    }
                    //   println("doublestr" + doublestr)
                    var selColumns = doublestr.split(",").toList
                    //  println("selColumns" + selColumns)
                    //   /*currentTempPseudoMapDF = */ pseudoTextDF.select(selColumns.head, selColumns.tail: _*).show(2000)
                    currentTempPseudoMapDF = pseudoTextDF.select(selColumns.head, selColumns.tail: _*)
                    //   println("currentTempPseudoMapDF")
                    //  currentTempPseudoMapDF.show
                    for (i <- 0 until idColumns.length) {
                      //currentTempPseudoMapDF = currentTempPseudoMapDF.withColumn("TEMP_" + idColumns(i), EncryptionUDF(pseudoTextDF(idColumns(i))))
                      currentTempPseudoMapDF = currentTempPseudoMapDF.withColumn("TEMP_" + idColumns(i), EncryptionUDF(currentTempPseudoMapDF("TEMP_" + idColumns(i))))
                    }
                    currentPseudoMapDF = currentTempPseudoMapDF
                    //   println("currentPseudoMapDF")
                    //   currentPseudoMapDF.show
                    pseudoTextTempDF = pseudoTextDF
                    var dropcol = ""
                    for (i <- 0 until idColumns.length) {
                      dropcol = "TEMP_" + idColumns(i)
                      pseudoTextTempDF = pseudoTextTempDF.drop(pseudoTextTempDF(dropcol))
                    }
                    currDataReplacedWithPseudoIdDF = pseudoTextTempDF
                    //    println("currDataReplacedWithPseudoIdDF")
                    //  currDataReplacedWithPseudoIdDF.show
                    textDF = currDataReplacedWithPseudoIdDF
                  }
                }
                //   textDFWithAge = textDF
                //    println("final DF after all deidentification specified in table")
                //   textDFWithAge.show
              }
              textDFWithAge = textDF
              println("final DF after all deidentification specified in table")
              textDFWithAge.show

              if (textSchema.toUpperCase().contains("DATALAKE_ID")) {
                textDF2 = textDFWithAge.withColumn("TEMP_NAME1", textDFWithAge("DATALAKE_ID")).withColumn("DATALAKE_ID", pseudoIdGeneration(textDFWithAge("DATALAKE_ID")))
                val pseudoTextDF2 = DataLakeIDGeneration.xRefIfDatalakeIDExistsinHBase("DATALAKE_ID", spark , spark.sqlContext, textDF2,
                  readProp.getProperty(EnvConstants.DataIngestionConstantsFile))
                currentPseudoMapDF = pseudoTextDF2.select(pseudoTextDF2("TEMP_NAME1"), pseudoTextDF2("DATALAKE_ID")).withColumn("TEMP_NAME1", EncryptionUDF(pseudoTextDF2("TEMP_NAME1")))
                currDataReplacedWithPseudoIdDF = pseudoTextDF2.drop("TEMP_NAME1")
                textDFWithAge = currDataReplacedWithPseudoIdDF

              } else if (isDatalakeID.equalsIgnoreCase("true")) {
                val columnName = replacementValue.split(",")
                textDF2 = textDFWithAge.withColumn("TEMP_NAME1", textDFWithAge(columnName(0))).withColumn("TEMP_NAME2", textDFWithAge(columnName(1))).withColumn(columnName(1), pseudoIdGeneration(textDFWithAge(columnName(1))))
                val pseudoTextDF2 = DataLakeIDGeneration.xRefIfDatalakeIDExistsinHBase(columnName(1), spark, spark.sqlContext, textDF2,
                  readProp.getProperty(EnvConstants.DataIngestionConstantsFile))
                currentPseudoMapDF = pseudoTextDF2.select(pseudoTextDF2("TEMP_NAME1"), pseudoTextDF2(columnName(0)), pseudoTextDF2("TEMP_NAME2"), pseudoTextDF2(columnName(1))).withColumn("TEMP_NAME1", EncryptionUDF(pseudoTextDF2("TEMP_NAME1"))).withColumn("TEMP_NAME2", EncryptionUDF(pseudoTextDF2("TEMP_NAME2")))
                currDataReplacedWithPseudoIdDF = pseudoTextDF2.withColumnRenamed(columnName(1), "DATALAKE_ID").drop("TEMP_NAME2").drop(columnName(0)).drop("TEMP_NAME1")
                textDFWithAge = currDataReplacedWithPseudoIdDF
                if (isColumnReplaced.equalsIgnoreCase("true")) {
                  schemaForMapping = textSchemaBeforeReplaced.split(",")
                }
              }
              var currDataReplacedWithPseudoIdDF2 = textDFWithAge.select(schemaForMapping.head, schemaForMapping.tail: _*)
              if (changedSchemardd.count() > 0) {
                for (changedSchema <- masterSchemaColsArr2) {
                  if (currDataReplacedWithPseudoIdDF2.columns.contains(changedSchema)) {
                    val intermediateDF = currDataReplacedWithPseudoIdDF2.withColumn("ChangedColumn", textDFWithAge(changedSchema))
                    currDataReplacedWithPseudoIdDF2 = intermediateDF.drop(changedSchema).withColumnRenamed("ChangedColumn", changedSchema)
                  }
                }
              }
              val currDataReplacedWithPseudoIdDF3 = currDataReplacedWithPseudoIdDF2
              println("currDataReplacedWithPseudoIdDF3")
              println("outputDirPathTemp " + outputDirPathTemp)
              currDataReplacedWithPseudoIdDF3.show
              currDataReplacedWithPseudoIdDF3.write.parquet(outputDirPathTemp)
              currentPseudoMapDF.distinct.map { x => x.mkString("|") }.write.text(readProp.getProperty(EnvConstants.PSEUDODMAPPATH) + "/" + "load_timestamp=" + currentimestamp)

              //----------------------End of pseudo id and encryption logic
            } else {
              textDFWithAge = textDF
              textDFWithAge = textDFWithAge.select(schemaForMapping.head, schemaForMapping.tail: _*)
              if (changedSchemardd.count() > 0) {
                for (changedSchema <- masterSchemaColsArr2) {
                  if (textDFWithAge.columns.contains(changedSchema)) {
                    val intermediateDF = textDFWithAge.withColumn("ChangedColumn", textDFWithAge(changedSchema))
                    textDFWithAge = intermediateDF.drop(changedSchema).withColumnRenamed("ChangedColumn", changedSchema)
                  }
                }
              }
              textDFWithAge.write.parquet(outputDirPathTemp)
            }

            if (txn_month != "") {

              val filesToMove = outputDirPathTemp + "*.parquet"
              outputDirPath = hdfsOutputDirBasePath + "/" + feedName + "/date_month=" + date_month

              Seq("hdfs", "dfs", "-mv", filesToMove, outputDirPath).!
              Seq("hdfs", "dfs", "-rm", "-r", outputDirPathTemp).!

            }

            copySuccessFlag = "success"
          } catch {
            case e: Exception =>
              {
                landToRawFailReason = "failed while writing to Parquet. " + e.getMessage
                e.printStackTrace()
              }

          }
        } else {
          landToRawFailReason = landToRawFailReason1
          logger.info("Skipping writing file " + fileName + " in Parquet format, as lineage columns could not be added.")
        }

        if (lineageSuccessFlag != "Y") {
          landToRawFailReason = landToRawFailReason1
        }

        val duration = (System.nanoTime - timeInSecs) / 1e9d
        logger.info("----Time taken, in seconds, for movement of file " + fileName + " - " + duration)

        try {
          returnString = readAuditFile.filter { x => x.contains(fileName) }.take(1)(0)
        } catch {
          case e: Exception =>
            {
              logger.error("Could not find " + fileName + " in audit file - " + e.getMessage)
              e.printStackTrace()
            }

        }
        if (!(returnString.isEmpty() || returnString.equalsIgnoreCase("") || returnString.equalsIgnoreCase(" "))) {
          returnString = returnString + "|" + copySuccessFlag + "|" + landToRawFailReason + "|" + duration.toString()
          //append audit file info to list file line.
          auditFileInfo.+=(returnString)
        }
      } else {
        logger.error("The File " + inputDirPathFile + " is Empty!!")
        emptyFilesListArr += fileName
      }
    }

    //save the audit file info buffer to the audit path auditfileOutputPath
    try {
      val tempAuditPath = auditFileAbsPath + "/" + feedName + "/"
      sparkContext.parallelize(auditFileInfo, 1).saveAsTextFile(tempAuditPath)
      fs.rename(new Path(tempAuditPath + "/part-00000"), new Path(tempAuditPath + "/" + feedName + "-00000"))

      //below is done to make sure re-writing audit file is successful. (since hadoop does not allow overwriting )
      val auditFileToMove = tempAuditPath + "/" + feedName + "-00000"
      Seq("hdfs", "dfs", "-mv", auditFileToMove, auditFileAbsPath).!
      fs.delete(new Path(tempAuditPath), true)

      //meta data for partitions created. to be used by DQ to identify current load partition
      val partitionMetaInfo = "date_month=" + date_month + "," + pLoadType + "," + partialFrmDate + "," + partialToDate
      val os = fs.create(new Path(metaDataLoc + "/" + feedName + "_feedmetadata.txt"))
      //below formatting is done to remove un-wanted text from output. Sample - ArrayBuffer(PTD_PROCEDURE/date_month=201607, PTD_PROCEDURE/date_month=201607, PTD_PROCEDURE/date_month=201607)
      //val patitionMeta = partitionMetaInfo.toString().split("").drop(13).dropRight(1).mkString("")
      os.writeBytes(partitionMetaInfo)

      spark.sql(readProp.getProperty(CommonConstants.DetailedAuditDB))
      spark.sql(readProp.getProperty(CommonConstants.DetailedAuditTable))
      spark.sql(readProp.getProperty(CommonConstants.DTLMSCK))

      if (emptyFilesListArr.length > 0) {
        val emptyFilePath = new Path(metaDataLoc + "/" + feedName + "_EmptyFiles.txt")
        if (fs.exists(emptyFilePath)) {
          fs.delete(emptyFilePath, true)
        }
        val osEmptyFiles = fs.create(emptyFilePath)
        osEmptyFiles.writeBytes(emptyFilesListArr.mkString("|"))
      }

      if (readProp.getProperty(EnvConstants.ENCRYPTIONFLAG).toUpperCase == "Y") {
        //val pseudoMapPath = readProp.getProperty(EnvConstants.PSEUDOIDMAPLOC)+"/"+readProp.getProperty(EnvConstants.PSEUDODMAPPATH)
        //create pseudo id map table 
        //hiveContext.sql(readProp.getProperty(CommonConstants.DetailedAuditDB))
        spark.sql(readProp.getProperty(EnvConstants.PSEUDOIDMAPTABLE))
        //hiveContext.sql(pseudoMapPath)   
        spark.sql(readProp.getProperty(EnvConstants.PSEUDOIDMAPLOC))
        spark.sql(readProp.getProperty(EnvConstants.PSEUDOMAPDBCREATION))
        spark.sql(readProp.getProperty(EnvConstants.CREATEPSEUDOIDMAPTABLE))
        spark.sql(readProp.getProperty(EnvConstants.PSEUDOMAPTBLMSCK))

        //Creation of view for distinct pseudo ids on PseudoMapTable.
        if (encrptyIndivIDExists) {
          spark.sql(readProp.getProperty(EnvConstants.IndivIdMapView))
          spark.sql(readProp.getProperty(EnvConstants.CreateIndivIdMapView))
        }
        if (encrptyPatientIDExists) {
          spark.sql(readProp.getProperty(EnvConstants.PateintIdMapView))
          spark.sql(readProp.getProperty(EnvConstants.CreatePateintIdMapView))
        }

      }

    } catch {
      case e: Exception =>
        {
          e.printStackTrace()
        }

    }
    if (exceptionEncountered) {
      stopSparkContext()
      System.exit(1)
    } else {
      logger.info("Stopping spark context...")
      stopSparkContext()

    }
  }

}