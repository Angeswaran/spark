package com.humira.datavalidation

/**
 * *****************************************************************************************
 *  @Author By CTS BIGDATA TEAM
 *  @Version 2.10.5
 *  @Description: Following steps are included in this class:
 * This class will give the Quality of data by performing checks on data.
 * 1. It performs the various checks on the data. DQ checks are listed below:
 *  •             Null check: there are fields which should not be NULL for ex: Market_code
 *  •             Valid Value check: checks whether column contains valid value. For ex: deletion_flag is a field which should only contain 0 or 1.
 *  •             Int check: checks if any integer column contains int or not
 *  •             Non-Numeric check :  checks Whether The String column has string value or not
 *  •             Look-up check :  check whether the mentioned column has present in reference file or not( Should Place Reference file location  for lookup check  and it will work on multiple columns)
 *  •             Uniqueness Check : checks  whether the column has duplicate value or not.
 *  •             Date Pattern Check :Should pass  date format (yyyyMMdd) as argument and it will check the date format for all the date columns in file.
 *  •             Blank Line Check: Should Check whether file has blank lines or not
 *  •             Record Count : It will give the count of records in file
 *  •             Column Count Check: it will take the column count and check with the schema, if columns are mismatching with schema we will consider as bad records
 *  •             Unique Badrecords : It will give unique bad records in file
 *  •             Count difference :  It will give the difference of records between two files(i.e. Historical file and Incremental file)
 *  •             Invalid value Check : checks whether the column has invalid value or not.
 *  •             Date Check : Check whether the date column is less than current date or not.
 *  2. DQ appends 2 columns to the file one is STATUS and the other one is EXPIRY_DATE.
 *     If any row fails any of the DQ checks its STATUS will be set as BAD RECORD along with the reason of failure otherwise the STATUS COLUMN is marked as GOOD.
 *     1 more column is appended at the end names EXPIRY_DATE its value depends on the value of DELETION_FLAG
 *     IF DELETION_FLAG =0THEN EXPIRY_DATE=99912(by default)
 *     IF DELETION_FLAG=1 THEN EXPIRY_DATE= CurrentYear + CurrentMonth
 *     The final output will be stored in parquet Format. For this you have to specify schema in properties files.
 *
 * **************************************************************************************
 */

import com.humira.constants.{ CommonConstants, EnvConstants, DataQualityConstants }
import java.text.SimpleDateFormat
import java.util.Properties
import org.apache.spark.sql.SQLContext
import scala.util.control.Exception.allCatch
import util.control.Breaks._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import scala.collection.mutable.Map
import java.util.Locale
import java.text.ParseException
import org.apache.spark.Accumulator
import java.util.Date
import java.io.IOException;
import java.io.Serializable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter
import java.io.File
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.Calendar
import com.humira.dataingestion.ReadPropeties
import scala.collection.mutable.Set
import org.apache.avro.reflect.Nullable
import org.apache.spark.SparkStatusTracker
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.OutputBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import java.io.OutputStreamWriter
import scala.io.Source
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.collection.parallel.immutable.ParSeq
import org.apache.spark.SparkFiles
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types._
import scala.sys.process._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

object DataQualityMgmt {
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
    var jobstatus = ""
  val spark = SparkSession.builder().appName("Data Quality").config("spark.sql.parquet.compression.codec", "snappy").getOrCreate()
  //val conf = new SparkConf().setAppName("Data Quality")
  //val sc = new SparkContext(conf)
  val sc = spark.sparkContext
  val conf1 = sc.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf1)
  //val sqlContext = new SQLContext(sc)
  //val hivecontext: HiveContext = new HiveContext(sc)
  //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
  import spark.implicits._

  val dateformatter = new SimpleDateFormat
  val format = new SimpleDateFormat("dd-MM-yyyy")
  var currentdate = format.format(Calendar.getInstance().getTime())
  val df = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss");
  val format1 = new SimpleDateFormat("dd-MM-yyyy HH-mm-ss")
val StartyearFormat = new SimpleDateFormat("yyyy")
val StartMonthFormat = new SimpleDateFormat("MMM")

var Starttimeformat = new SimpleDateFormat("HH:mm:ss");

  var currentdate1 = format1.format(Calendar.getInstance().getTime())
val calobj = Calendar.getInstance();
  val starttime = format1.format(calobj.getTime());
  val startYear = StartyearFormat.format(calobj.getTime());
  val startMonth = StartMonthFormat.format(calobj.getTime());
  var Statstartime = Starttimeformat.format(Calendar.getInstance().getTime())
val ingestion_dt = currentdate1
val currentimestamp: Long = System.currentTimeMillis / 1000
var Successflag: String = null
val DataQualityStats = collection.mutable.Map[String, String]()
var propsDQ: Properties = readDQPropeties(args(0))
var propsEnv: Properties = readEnvpropeties(args(2))
var dataSourceBc: org.apache.spark.broadcast.Broadcast[String] = null

val Market_CD = propsEnv.getProperty(CommonConstants.MARKETCD)
val Actualcountlocation = propsEnv.getProperty(EnvConstants.ACTUALCOUNTBASELOCATION) + "/" + propsDQ.getProperty(DataQualityConstants.feedName) + ".txt"
val loadNullAsGood = propsEnv.getProperty(EnvConstants.LOADNULLASGOOD)
var partitionColumn1: String = null
var partitionColumn2: String = null
var partitionColumn3: String = null
var partitionValues : Array[Any]= null
val dateFormat = propsEnv.getProperty(EnvConstants.DATEFORMAT) //"yyyy/MM/dd HH:mm:SS"
var nullcheckcols: String = null;
var numcheckcols: String = null;
var datecheckcols: String = null;
var DuplicateRecords: String = null;
var Nonnumeric: String = null
    var ValidDateCheck: String = null
    var ValidvalueCheck: String = null
    var Non_NumericCheck: String = null
    var errordesc = ""
    var PatientLookup: String = null
    var Lookupfields: String = null
    var InvalidValueCheck: String = null
    var duplicatecheckcols: String = null
    var Lookupcols: String = null
    var daterangecols: String = null
    var deletionflag: String = null
    //var stglocation: String = null
    //var rawfile2_count: Long = 0L
    var percentileRdd: String = null
    var RowdiffRdd: Long = 0
    var historyCount: Long = 0
    var totalHistoryCount: Long = 0
    var lastFullHistoryCount: Long = 0
    var processedRdd: RDD[String] = null
    var rddchange: RDD[String] = null
    var hisfilecountexists: Boolean = false
    var masterSchemaCols = ""
    var inSchemePath = propsEnv.getProperty(CommonConstants.INCOMINGSCHEMAPATH) + "/" + propsDQ.getProperty(CommonConstants.INCOMINGSCHEMAFILE)
    val sqoopedDatabase = propsEnv.getProperty(CommonConstants.SQOOPEDDATABASE)
    var encrptionFlag = propsEnv.getProperty(EnvConstants.ENCRYPTIONFLAG)
    val partialRefFlag = propsDQ.getProperty(CommonConstants.PARTIALREFFLAG).toUpperCase()
    var masterSchema = sc.textFile(inSchemePath).take(1)(0).split("\\|")
    var masterSchemaArr = ArrayBuffer[String]()
    var masterSchemaArrForDiff = ArrayBuffer[String]()
    for (ele <- 0 to (masterSchema.length - 1)) {
masterSchemaArr += masterSchema(ele).split(" ")(0)
    masterSchemaArrForDiff += masterSchema(ele).split(" ")(0)
    }

val schemaChangeTrackerPath = propsEnv.getProperty(EnvConstants.SCHEMACHANGETRACKERPATH) + "/" + propsDQ.getProperty(CommonConstants.MASTERSCHEMAFILENAME)
var masterSchemaColsArr2 = ArrayBuffer[String]()
val changedSchemardd = sc.textFile(schemaChangeTrackerPath)
if (changedSchemardd.count() > 0) {
  val masterSchemaArr2 = changedSchemardd.take(1)(0).split("\\|")
if (masterSchemaArr2.length > 0) {
  //extract only column names from it
  for (ele <- 0 to masterSchemaArr2.length - 1) {
    masterSchemaColsArr2 += masterSchemaArr2(ele).split(" ")(0)
  }
  for (changedSchema <- masterSchemaColsArr2) {
    if (masterSchemaArr.contains(changedSchema)) {
masterSchemaArr.remove(masterSchemaArr.indexOf(changedSchema))
    }
    //val replaceString = ","+changedSchema+","
    //masterSchemaCols = masterSchemaCols.replace(replaceString,",")
  }
}
}

var header = ""
    masterSchemaCols = masterSchemaArr.mkString(",")
    if (partialRefFlag.equalsIgnoreCase("Y")) {
header = masterSchemaCols + ",src_code,logical_src_code,raw_ingestion_time,active_flag"
    } else {
header = masterSchemaCols + ",src_code,logical_src_code,raw_ingestion_time"
    }
if (changedSchemardd.count() > 0) {
  for (changedSchema <- masterSchemaColsArr2) {
    if (masterSchemaArrForDiff.contains(changedSchema)) {
val concatString = "," + changedSchema
    header = header + concatString
    }
  }
}
header = header + ",date_month"

    val statsOutputPath = propsEnv.getProperty(EnvConstants.MetricsBASEPATH)

    if (!propsDQ.getProperty(DataQualityConstants.NULLCHECKCOLS).trim().equals("") || propsDQ.getProperty(DataQualityConstants.NULLCHECKCOLS).trim() != "") {
nullcheckcols = propsDQ.getProperty(DataQualityConstants.NULLCHECKCOLS).toUpperCase()
    } else {
nullcheckcols = null;
    }
if (!propsDQ.getProperty(DataQualityConstants.CHECKNUMERICCOLS).trim().equals("") || propsDQ.getProperty(DataQualityConstants.CHECKNUMERICCOLS).trim() != "") {
  numcheckcols = propsDQ.getProperty(DataQualityConstants.CHECKNUMERICCOLS).toUpperCase()
} else {
  numcheckcols = null;
}

if (!propsDQ.getProperty(DataQualityConstants.DATECHECKCOLS).trim().equals("") || propsDQ.getProperty(DataQualityConstants.DATECHECKCOLS).trim() != "") {
  datecheckcols = propsDQ.getProperty(DataQualityConstants.DATECHECKCOLS)
} else {
  datecheckcols = null
}

/*if (!propsDQ.getProperty(DataQualityConstants.NONNUMERIC).trim().equals("") || propsDQ.getProperty(DataQualityConstants.NONNUMERIC).trim() != "") {
      Nonnumeric = propsDQ.getProperty(DataQualityConstants.NONNUMERIC)
      println(Nonnumeric)
    } else {
      Nonnumeric = null
    }
 */
if (!propsDQ.getProperty(DataQualityConstants.VALIDDATECHECKCOLS).trim().equals("") || propsDQ.getProperty(DataQualityConstants.VALIDDATECHECKCOLS).trim() != "") {
  ValidDateCheck = propsDQ.getProperty(DataQualityConstants.VALIDDATECHECKCOLS)
} else {
  ValidDateCheck = null
}
if (!propsDQ.getProperty(DataQualityConstants.VALIDCHECKCLOUMNS).trim().equals("") || propsDQ.getProperty(DataQualityConstants.VALIDCHECKCLOUMNS).trim() != "") {
  ValidvalueCheck = propsDQ.getProperty(DataQualityConstants.VALIDCHECKCLOUMNS)
} else {
  ValidvalueCheck = null
}

if (!propsDQ.getProperty(DataQualityConstants.NON_NUMERIC).trim().equals("") || propsDQ.getProperty(DataQualityConstants.NON_NUMERIC).trim() != "") {
  Non_NumericCheck = propsDQ.getProperty(DataQualityConstants.NON_NUMERIC)

} else {
  Non_NumericCheck = null
}

if (!propsDQ.getProperty(DataQualityConstants.PATIENTLOOKUP).trim().equals("") || propsDQ.getProperty(DataQualityConstants.PATIENTLOOKUP).trim() != "") {
  PatientLookup = propsDQ.getProperty(DataQualityConstants.PATIENTLOOKUP).toUpperCase()
} else {
  PatientLookup = null;
}
if (!propsDQ.getProperty(DataQualityConstants.InvalidValue).trim().equals("") || propsDQ.getProperty(DataQualityConstants.InvalidValue).trim() != "") {
  InvalidValueCheck = propsDQ.getProperty(DataQualityConstants.InvalidValue).toUpperCase()
} else {
  InvalidValueCheck = null
}
if (!propsDQ.getProperty(DataQualityConstants.DUPLICATECHECKCOLS).trim().equals("") || propsDQ.getProperty(DataQualityConstants.DUPLICATECHECKCOLS).trim() != "") {
  duplicatecheckcols = propsDQ.getProperty(DataQualityConstants.DUPLICATECHECKCOLS).toUpperCase()
} else {
  duplicatecheckcols = null;
}

if (!propsDQ.getProperty(DataQualityConstants.LOOKUPFILELOCATION).trim().equals("") || propsDQ.getProperty(DataQualityConstants.LOOKUPFILELOCATION).trim() != "") {
  var lookUpArray = propsDQ.getProperty(DataQualityConstants.LOOKUPFILELOCATION).split("\\,")
var lookUpColInfo1 = ""
var tbName = ""
var colname = ""
for (lookUp <- lookUpArray) {
  tbName = lookUp.split("\\;")(0)
if (!lookUp.split("\\;")(1).equalsIgnoreCase("sqoop")) {

if (lookUp.split("\\;").length < 3) {
if(loadNullAsGood.equalsIgnoreCase("Y")){
colname = tbName + ":" + lookUp.split("\\;")(1).split("\\=")(0) + ":" + lookUp.split("\\;")(1).split("\\=")(1)
} else {
colname = tbName + ":" + lookUp.split("\\;")(1).split("\\=")(1)
}
} else {
var compositeColsWithoutTable = lookUp.split("\\;").drop(1)
var compositeColsElements = ""
for (compcol <- compositeColsWithoutTable) {
  if(loadNullAsGood.equalsIgnoreCase("Y")){
  compositeColsElements = compositeColsElements + compcol.split("\\=")(0) + ":" + compcol.split("\\=")(1) + ":"
  } else {
  compositeColsElements = compositeColsElements + compcol.split("\\=")(1) + ":"
  }
}
colname = tbName + ":" + "(" + compositeColsElements.substring(0, compositeColsElements.length() - 1) + ")"
}
} else if (lookUp.split("\\;")(1).equalsIgnoreCase("sqoop")) {
if (lookUp.split("\\;").length < 4) {
 if(loadNullAsGood.equalsIgnoreCase("Y")){
colname = tbName + ":" + lookUp.split("\\;")(2).split("\\=")(0) + ":" + lookUp.split("\\;")(2).split("\\=")(1)
} else {
 colname = tbName + ":" + lookUp.split("\\;")(2).split("\\=")(1)
}
} else {
var compositeColsWithoutTable = lookUp.split("\\;").drop(2)
var compositeColsElements = ""
for (compcol <- compositeColsWithoutTable) {
    if(loadNullAsGood.equalsIgnoreCase("Y")){
  compositeColsElements = compositeColsElements + compcol.split("\\=")(0) + ":" + compcol.split("\\=")(1) + ":"
  } else {
  compositeColsElements = compositeColsElements + compcol.split("\\=")(1) + ":"
  }
}
colname = tbName + ":" + "(" + compositeColsElements.substring(0, compositeColsElements.length() - 1) + ")"
}
}

  lookUpColInfo1 = lookUpColInfo1 + colname + ","
}

  Lookupcols = lookUpColInfo1.substring(0, lookUpColInfo1.length() - 1)

} else {
  Lookupcols = null
}

if (!propsDQ.getProperty(DataQualityConstants.DATERANGE).trim().equals("") || propsDQ.getProperty(DataQualityConstants.DATERANGE).trim() != "") {
  daterangecols = propsDQ.getProperty(DataQualityConstants.DATERANGE).toUpperCase()
} else {
  daterangecols = null;
}
if (!propsDQ.getProperty(DataQualityConstants.DELETIONFLAG).trim().equals("") || propsDQ.getProperty(DataQualityConstants.DELETIONFLAG).trim() != "") {
  deletionflag = propsDQ.getProperty(DataQualityConstants.DELETIONFLAG).toUpperCase()
} else {
  deletionflag = null;
}
/*if (!propsEnv.getProperty(EnvConstants.HISTORYFILE).trim().equals("") || propsEnv.getProperty(EnvConstants.HISTORYFILE).trim() != "") {
      stglocation = propsEnv.getProperty(EnvConstants.HISTORYFILE).toUpperCase()
    } else {
      stglocation = "";
    }*/
val srcFilepath = propsEnv.getProperty(EnvConstants.RAWBASEPATH)
    var partitionCol12 = ""
    var partitionCol2 = ""
    var dts: String = ""
    var tss: String = ""
    var dtsBc: org.apache.spark.broadcast.Broadcast[String] = null
    var tssBc: org.apache.spark.broadcast.Broadcast[String] = null

    val feedmetadata = sc.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/" + propsDQ.getProperty(DataQualityConstants.FEEDMETADATALOC)).take(1).mkString(",")
    val feedLoadType = sc.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/loadmetadata.txt").take(1)(0).split("\\,")(2)

    if (feedmetadata.isEmpty || feedmetadata == null) {
logger.error("No lines found in the feedmetadata file ..")
throw new RuntimeException("No lines found in the feedmetadata file ..")
    }
var srcFilepath_tmp: String = null

    var partialFrmDate = ""
    var partialToDate = ""
    val feedMetadataArr = feedmetadata.split(",")
    //val date_month = feedMetadataArr(0)
    val partitionCol = feedMetadataArr(0)
    val loadType = feedMetadataArr(1)
    if (loadType == "P") {
partialFrmDate = feedMetadataArr(2)
    partialToDate = feedMetadataArr(3)
    }
//srcFilepath_tmp = srcFilepath + "/" + propsDQ.getProperty(DataQualityConstants.feedName) + "/" + date_month + "/*"
srcFilepath_tmp = srcFilepath + "/" + propsDQ.getProperty(DataQualityConstants.feedName) + "/"
println("******************srcFilepath_tmp***********************")
println(srcFilepath_tmp)

    var splitString = propsDQ.getProperty(CommonConstants.RAWTBLDELIMITER)
    val listFilePath = propsEnv.getProperty(CommonConstants.FILELISTCURRENTABSOLUTEPATH) + "/" + propsDQ.getProperty(CommonConstants.FILELISTNAME)
    val listFileRDD = sc.textFile(listFilePath)
    val listFileArray = listFileRDD.collect() //collect RDD into an Array
    val loadTypeForCountCal = listFileArray(0).split(splitString)(4)
    val delimeter = propsDQ.getProperty(CommonConstants.RAWTBLDELIMITER)
    val dataSetFromEnv = propsEnv.getProperty(CommonConstants.DATASET)
    val vendorFromEnv = propsEnv.getProperty(CommonConstants.VENDOR)
    val data_Set = vendorFromEnv.toUpperCase() + "_" + dataSetFromEnv.toUpperCase()
    splitString = "\\" + splitString
    val isStringQualifierDoubleQuotes = propsDQ.getProperty(CommonConstants.ISSTRINGQUALIFIERDOUBLEQUOTES)

    if(!(isStringQualifierDoubleQuotes.equalsIgnoreCase("Y"))){
splitString = splitString + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    }
// DataFrame will Read Parquet File
//var ParquetDf = spark.read.parquet(srcFilepath_tmp)
var ParquetDf1 = spark.read.parquet(srcFilepath_tmp)

var partitionValue = partitionCol.split("=")(1)
var partitionColumn = partitionCol.split("=")(0)

var ParquetDf = ParquetDf1.filter(ParquetDf1(partitionColumn) === partitionValue)
println("***********************ParquetDf********************")
ParquetDf.show(20)

    // ----> Read Parquet file
    var convertdftordd = ParquetDf.rdd
    var rawfile = convertdftordd.map { x => x.mkString(delimeter) }
//var rawfile = convertdftordd.map { x => x.mkString("|") }
val rawfile_count = rawfile.count()

    /*
     * @Method Name: isNullorBlank
     * This Method will check whether the Columns are null or blank
     * We will compare  String is empty or not
     */
    def isNullorBlank(x: String) = {
x == null || x.isEmpty() || x.trim.equals("") || x.toLowerCase == "null"
}
/*
 * @Method Name: isNonNumeric
 *
 * This Method will check the Alpha-Numeric Values in the columns
 * We will Match the Columns with regex,it will check the pattern if the columns
 */
def isaplhaNumeric(x: String) = {
    try {
if (x.matches("^([A-Za-z]?=[0-9])+$")) {
false
} else {
true
}
    } catch {
    case parseException: ParseException => false
    case t: Exception                   => false
    }
}
/*
 * @Method Name : isLineBlank
 *
 * This Method will check whether file contains Blank Line or not
 * will compare the whole row is  empty and null
 */

def isLineBlank(x: String): Boolean = {
    var checkblank = false
try {
val x1 = x.split(splitString)
//val arrayinfo = x1.dropRight(2).mkString("|")
val arrayinfo = x1.dropRight(2)

var counter = 0
for (col <- arrayinfo) {
  if (col == null || col.isEmpty() || col.trim.equals("") || col.toLowerCase == "null" || col.toUpperCase() == "NULL" || col.equalsIgnoreCase("NULL") || col.equalsIgnoreCase("null")) {
counter += 1
  }
}
if (counter == arrayinfo.length) {
checkblank = true
}

} catch {
case parseException: ParseException => false
case t: Exception                   => false
}
    return checkblank
}

/*
 * @Method Name : isValidValue
 *
 * This Method will Check the specified column has valid Values or not
 * we will compare the column with the specified valid values
 */

def isValidValue(x: String) = {
    if (x.equals(DataQualityConstants.VALID_VALUES1) || x.equals(DataQualityConstants.VALID_VALUES2) || x.equals(DataQualityConstants.epsilon)) {
true
    } else {
false

    }
}

def isInValidValue(x: String) = {
    try {
if (x.equals(0) || x.equals("") || x == null) {
false
} else {
true
}
    } catch {
    case parseException: ParseException => false
    case t: Exception                   => false
    }
}

/*
 * @Method Name : isHeaderValid
 *
 *    //Check Header
 * This Method will check Whether The header in the file is valid or not
 * It will Check The header which we Specify in Properties File and match Header with file header
 */

def isHeaderValid(headerinfilex: String): Boolean = {
    val foundHeaderList = headerinfilex.split(",")
//val actualHeader = propsDQ.getProperty(DataQualityConstants.HEADER).toUpperCase()
val expectedHeaderList = header.split(",").toList
foundHeaderList.zip(expectedHeaderList).forall { case (x, y) => x.toLowerCase == y.toLowerCase }
}

/*
 * @Method Name :getHeaderColCount
 *
 * This Method will the Header count persent in file
 */

def getHeaderColCount() = {
    header.split(",").length
}

/*
 * @Method Name :getMApIndexColumn
 *
 * This Method will take Input as Map,the Key values are (String,Int)
 * We will call expectedHeader and will split by "," '
 * We will use for Loop where we iterate columns, and we will map the column to columnIndexMap
 * if it matches it will take as key value pair and we will return columnIndexMap
 *
 */

def daterange(x: String): Boolean = {
    if ((x < propsEnv.getProperty(EnvConstants.DateRange1)) || (x > propsEnv.getProperty(EnvConstants.DateRange2))) {
return true
    } else
false
}

def getMApIndexColumn(): Map[String, Int] = {
    val columnIndexMap = Map[String, Int]()
val actualColumns = header.toUpperCase().split(",")
var count = 0

for (column <- actualColumns) {
columnIndexMap += (column.trim() -> count)
count += 1
}
    columnIndexMap
}

val fieldIndexMap = getMApIndexColumn()

    // Accumalators to store bad records
    val toUniqueBadrecAccum = sc.accumulator(0)
    val colAccum = sc.accumulator(0)
    val blankLineAccum = sc.accumulator(0)
    val columCountAccum = sc.accumulator(0)
    val validDateAccum = sc.accumulator(0)
    val blankAcuum = sc.accumulator(0)
    val percentageNullAcumm = sc.accumulator(0)
    var AccArrayforNull: Array[Accumulator[Int]] = null
    var AccArrayforNum: Array[Accumulator[Int]] = null
    var AccArrayforDate: Array[Accumulator[Int]] = null
    var AccArrayforDuplicate: Array[Accumulator[Int]] = null
    var AccArrayforNonnumeric: Array[Accumulator[Int]] = null
    var AccArrayForValidDate: Array[Accumulator[Int]] = null
    var AccArrayforValidValue: Array[Accumulator[Int]] = null
    var AccArrayForNonNumeric: Array[Accumulator[Int]] = null
    var AccArrayForValidCurrentMonth: Array[Accumulator[Int]] = null
    var AccArrayforLookup: Array[Accumulator[Int]] = null
    var AccArrayforInvalidValue: Array[Accumulator[Int]] = null
    //var AccArrayforLookupcols: Array[Accumulator[Int]] = null
    var AccArrayforDateRangeCols: Array[Accumulator[Int]] = null
    var AccArrayforLookupcol = sc.accumulator(0)
    var DuplicateAccumulator = sc.accumulator(0)
    var UniqueBadrecAccum = sc.accumulator(0)
    //var something: Int = 0
    if (nullcheckcols != null) {
val nullColArray = nullcheckcols.split("\\,")
AccArrayforNull = new Array[Accumulator[Int]](nullColArray.length)
initializeAccum(AccArrayforNull)

    }

    if (numcheckcols != null) {
val numcheckcolsArray = numcheckcols.split("\\,")
AccArrayforNum = new Array(numcheckcolsArray.length)
initializeAccum(AccArrayforNum)
    }

    if (datecheckcols != null) {
val datecheckcolsArray = datecheckcols.split(",")
AccArrayforDate = new Array(datecheckcolsArray.length)
initializeAccum(AccArrayforDate)
    }
    if (DuplicateRecords != null) {

val DuplicateRecordsArray = DuplicateRecords.split(",")
AccArrayforDuplicate = new Array(DuplicateRecordsArray.length)
initializeAccum(AccArrayforDuplicate)
    }

    if (Nonnumeric != null) {

val NonnumericArray = Nonnumeric.split(",")
AccArrayforNonnumeric = new Array(NonnumericArray.length)
initializeAccum(AccArrayforNonnumeric)
    }

    if (ValidDateCheck != null) {
val ValidDateCheckArray = ValidDateCheck.split(",")
AccArrayForValidDate = new Array(ValidDateCheckArray.length)
initializeAccum(AccArrayForValidDate)
    }
    if (ValidvalueCheck != null) {
val ValidValueCheckArray = ValidvalueCheck.split(",")
AccArrayforValidValue = new Array(ValidValueCheckArray.length)
initializeAccum(AccArrayforValidValue)
    }

    if (Non_NumericCheck != null) {
val NonNumericArray = Non_NumericCheck.split(",")
AccArrayForNonNumeric = new Array(NonNumericArray.length)
initializeAccum(AccArrayForNonNumeric)
    }

    if (PatientLookup != null) {
val lookupColArray = PatientLookup.split("\\,")
AccArrayforLookup = new Array[Accumulator[Int]](lookupColArray.length)
initializeAccum(AccArrayforLookup)
    }

    if (InvalidValueCheck != null) {
val InvalidValueArray = InvalidValueCheck.split("\\,")
AccArrayforInvalidValue = new Array[Accumulator[Int]](InvalidValueArray.length)
initializeAccum(AccArrayforInvalidValue)
    }

    /*
    //AccArrayforLookupcols
    if (Lookupcols != null) {
      val lookupcolsArray = Lookupcols.split("\\,")
      AccArrayforLookupcols = new Array[Accumulator[Int]](lookupcolsArray.length)
      initializeAccum(AccArrayforLookupcols)
    }
     *
     */

    if (daterangecols != null) {
val DateRangeColsArray = daterangecols.split("\\,")
AccArrayforDateRangeCols = new Array[Accumulator[Int]](DateRangeColsArray.length)
initializeAccum(AccArrayforDateRangeCols)
    }

    def initializeAccum(accArr: Array[Accumulator[Int]]) = for (acc <- 0 to accArr.length - 1) {
accArr(acc) = sc.accumulator(0)
    }
    /*
     * @Method Name : checkColCount
     *
     * This method will check the column count.
     * it will match the column count with getHeaderColCount, if it matches it will return true
     */
    def checkColCount(length: Int) = {
if (length != getHeaderColCount) {
false
} else {
true
}
    }
    /*
     *  @Method Name : isNumber
     *
     *  This Methid Will Check whether the column is Numeric or not
     */
    // numeric check colmns
    def isNumber(s: String): Boolean = {
if (s != null && !s.equals("")) {
(allCatch opt s.toDouble).isDefined
} else {
true
}
    }

    /*
     * @Method Name: checkDateFormat
     * It will Check the DateFormat,and will parse the DateFormate to String
     *
     */

    def checkDateFormat(column: String, dateFormat: String): Boolean = {
if (column == null || column.equals("")) {
return true
}

val dateformatter = new SimpleDateFormat(dateFormat.trim, Locale.ENGLISH)
dateformatter.setLenient(false) //yyyymmdd
try {
dateformatter.parse(column.toString().trim())

true

} catch {
case parseException: ParseException => false
case t: Exception                   => false
}
    }

    /*
     * @Method Name: isvalid
     *
     * This Method will check whether the date in date column is valid or not using Regex
     */

    def isvalid(dateFormat: String): Boolean = {

if (dateFormat == null || !dateFormat.matches(("\\d{4}-[01]\\d-[0-3]\\d")))
false
try {
dateformatter.parse(dateFormat)
return true
} catch {
case parseException: ParseException => false
case t: Exception                   => false
}
    }

    /*
     * @Method Name: checkvalidDate
     *
     * This Method Will Check Whether Specifed Date Column is less than the current date or not.
     * If it is less it will return true
     */

    /*def checkvalidDate(column: String, dateFormat: String): Boolean = {
      if (column == null || column.equals("")) {
        return true
      }

      val dateformatter = new SimpleDateFormat(dateFormat.trim, Locale.ENGLISH)
      dateformatter.setLenient(false) //yyyymmdd
      try {
        val formatter = dateformatter.parse(column.toString().trim())
        val Date1 = new Date()

        val currentdate2 = dateformatter.parse(Date1.toString())
        if (currentdate2.equals(formatter) || currentdate2.after(formatter)) {
          false
        } else {
          true
        }
      } catch {
        case parseException: ParseException => false
        case t: Exception => false
      }
    }*/

    def checkvalidDate(column: String, dateFormat: String): Boolean = {

if (column == null || column.equals("")) {
return true
}
val dateformatter = new SimpleDateFormat(dateFormat.trim, Locale.ENGLISH)
dateformatter.setLenient(false) //yyyymmdd

try {
val inputformatting = dateformatter.parse(column.toString())
val today = Calendar.getInstance().getTime();
val formattered = new SimpleDateFormat("ddMMMyyyy:HH:mm:ss", Locale.ENGLISH);
val formatting = formattered.format(today)

if (inputformatting.equals(formatting) || inputformatting.toString() > (formatting)) {
  true
} else {
  false
}
} catch {
case parseException: ParseException => false
case t: Exception                   => false
}
    }

    /*
     * @Method Name:Nonnumerics
     *
     * This Mehod will check whether the given coulmn is non-numeric or numreic
     * if the column is not converting to Double then the if condition will return true,
     * Which means the given column is non-numeric
     */
    def nonNumerics(s: String): Boolean = {
if (s != null && !s.equals("")) {
!(allCatch opt s.toDouble).isDefined
} else {
true
}
    }

    /*def getMd5(fileheader : String)={
    try{
    val digest = MessageDigest.getInstance("MD5")
     val md5hash1 = digest.digest(fileheader.getBytes).map("%02x".format(_)).mkString
     md5hash1
    }
    catch{
      case e:Exception =>
        logger.error("Header Mistmatch")
        sys.exit(1)
    }
  }*/

    def getLength(x: String): Boolean = {
if (x.length() == propsDQ.getProperty(DataQualityConstants.Lengthofcolumn).split(";")(1).toInt) {
false
} else {
true
}
    }
    def minMax(a: Array[Int]): (Int, Int) = {
if (a.isEmpty) throw new java.lang.UnsupportedOperationException("array is empty")
a.foldLeft((a(0), a(0))) { case ((min, max), e) => (math.min(min, e), math.max(max, e)) }
    }

    /* def changeSchemaType(x:String): STRUCT= {

       if(x!=null){
         x match{

           case x => x == String
         }
         return StructField(fieldName, StringType, true)
         else{

         }
       }

     }*/

    /*This will perform a Left Outer Join of the Table with the Lookup Tables based on Join Condition specified in LOOKUPFILELOCATION and generates a DataFrame
     * by adding additional columns which are further used for updating the dqm status and lookupfail count
     */
    var lookUpCheckMap = Map[String, Int]()
var lookupCheckCount = 0
var dfjoin = ParquetDf
//.persist(StorageLevel.MEMORY_AND_DISK_SER)
var parquetDf1 = spark.emptyDataFrame
var lookupKeys: Array[String] = null
var lookupTableCounter = 0
var filterNullDF = spark.emptyDataFrame
var filterDF = spark.emptyDataFrame
var joinedDF = spark.emptyDataFrame
var unionDF = spark.emptyDataFrame

if(loadNullAsGood.equalsIgnoreCase("Y")){

if (PatientLookup != null) {
if (PatientLookup.equalsIgnoreCase("Y")) {
val basePath = propsEnv.getProperty(EnvConstants.RAWBASEPATH)
for (lookupTableValues <- propsDQ.getProperty(DataQualityConstants.LOOKUPFILELOCATION).split(",")) {
 // lookupCheckCount = lookupCheckCount + 1
val lookupTable = lookupTableValues.split(";")(0)
//lookUpCheckMap(lookupTable) = lookupCheckCount
if (!lookupTableValues.split(";")(1).equalsIgnoreCase("sqoop")) {
lookupKeys = lookupTableValues.split(";").drop(1)
    parquetDf1 = broadcast(spark.read.parquet(basePath + "/" + lookupTable))
}else{
lookupKeys = lookupTableValues.split(";").drop(2)
    //val hc = new HiveContext(sc)
    spark.sql("use " + sqoopedDatabase)
    parquetDf1 = broadcast(spark.sql("select * from " + lookupTable))
}
  var joinKey: Column = null
var nullColumnFilter:Column = null
var notNullColumnFilter:Column = null
var lookUpKeyForPickup = ""
var lookUpKeyForPickupAlias = ""
if (lookupKeys.length == 1) {
for (lookupKey <- lookupKeys) {
//var filterNullDF = sqlContext.emptyDataFrame
//var filterDF = sqlContext.emptyDataFrame
//var joinedDF = sqlContext.emptyDataFrame

val firstlookupKey = lookupKey.split("\\=")(0)
val secondlookupKey = lookupKey.split("\\=")(1)
joinKey = upper(dfjoin(firstlookupKey)) === upper(parquetDf1(secondlookupKey))
lookUpKeyForPickup = secondlookupKey
lookUpKeyForPickupAlias = lookupTable + ":" + firstlookupKey + ":" + secondlookupKey

lookupCheckCount = lookupCheckCount + 1
lookUpCheckMap(lookupTable + ":" + firstlookupKey + ":" + secondlookupKey) = lookupCheckCount

if(lookupTableCounter == 0)
{
  filterNullDF = dfjoin.filter(dfjoin(firstlookupKey).isNull || dfjoin(firstlookupKey).===(""))
  //filterDF = dfjoin.except(filterNullDF)
  joinedDF = dfjoin.filter(dfjoin(firstlookupKey).isNotNull || dfjoin(firstlookupKey).!==("")).as("d1").join(parquetDf1.as("d2"), joinKey, "left_outer").select($"d1.*", parquetDf1(lookUpKeyForPickup) as lookUpKeyForPickupAlias)
}
else
{
  filterNullDF = unionDF.filter(unionDF(firstlookupKey).isNull || unionDF(firstlookupKey).===("") )
  //filterDF = unionDF.except(filterNullDF)
  joinedDF = unionDF.filter(unionDF(firstlookupKey).isNotNull || unionDF(firstlookupKey).!==("")).as("d1").join(parquetDf1.as("d2"), joinKey, "left_outer").select($"d1.*", parquetDf1(lookUpKeyForPickup) as lookUpKeyForPickupAlias)
}
var filterNullRDDEmpty = filterNullDF.rdd.isEmpty
if (! filterNullRDDEmpty) {
  filterNullDF = filterNullDF.withColumn(lookUpKeyForPickupAlias,lit(9999))
}
// joinedDF = filterDF.as("d1").join(parquetDf1.as("d2"), joinKey, "left_outer").select($"d1.*", parquetDf1(lookUpKeyForPickup) as lookUpKeyForPickupAlias)
if (! filterNullRDDEmpty) {
  unionDF = joinedDF.unionAll(filterNullDF)
} else {
  unionDF = joinedDF
}
unionDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
logger.info("Union Dataframe for Single LookUp Check")
logger.info(unionDF.show(3))
logger.info("DQM_INFO: Check Count for Loop No : "+lookupTableCounter+" is "+unionDF.count())
}
}
else
{
for (lookupKey <- lookupKeys) {
val firstlookupKey = lookupKey.split("\\=")(0)
val secondlookupKey = lookupKey.split("\\=")(1)
if (joinKey == null) {
  joinKey = upper(dfjoin(firstlookupKey)) === upper(parquetDf1(secondlookupKey))
  lookUpKeyForPickup = secondlookupKey
  lookUpKeyForPickupAlias = lookupTable + ":" + firstlookupKey + ":" + secondlookupKey

lookupCheckCount = lookupCheckCount + 1
lookUpCheckMap(lookupTable + ":" + firstlookupKey + ":" + secondlookupKey) = lookupCheckCount

if (nullColumnFilter == null){
  nullColumnFilter = (dfjoin(firstlookupKey).isNull || dfjoin(firstlookupKey).===(""))
}

} else {
  joinKey = joinKey && upper(dfjoin(firstlookupKey)) === upper(parquetDf1(secondlookupKey))
 // nullColumnFilter = nullColumnFilter && (dfjoin(firstlookupKey).isNull || dfjoin(firstlookupKey).===(""))
   nullColumnFilter = nullColumnFilter || (dfjoin(firstlookupKey).isNull || dfjoin(firstlookupKey).===(""))
}
}

    //var filterNullDF = sqlContext.emptyDataFrame
    //var filterDF = sqlContext.emptyDataFrame
    //var joinedDF = sqlContext.emptyDataFrame

    if(lookupTableCounter == 0){
      filterNullDF = dfjoin.filter(nullColumnFilter)
      filterDF = dfjoin.except(filterNullDF)
    }else{
      filterNullDF = unionDF.filter(nullColumnFilter)
      filterDF = unionDF.except(filterNullDF)
    }
    var filterNullRDDEmpty = filterNullDF.rdd.isEmpty
    if (! filterNullRDDEmpty) {
      filterNullDF = filterNullDF.withColumn(lookUpKeyForPickupAlias,lit(9999))
    }
    joinedDF = filterDF.as("d1").join(parquetDf1.as("d2"), joinKey, "left_outer").select($"d1.*", parquetDf1(lookUpKeyForPickup) as lookUpKeyForPickupAlias)
    if (! filterNullRDDEmpty) {
      unionDF = joinedDF.unionAll(filterNullDF)
    } else {
      unionDF = joinedDF
    }

    //unionDF = joinedDF.unionAll(filterNullDF)
    unionDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    logger.info("Union Dataframe for Multiple LookUp Checks")
    logger.info(unionDF.show(3))
        logger.info("DQM_INFO: Check Count for Loop No : "+lookupTableCounter+" is "+unionDF.count())
}
lookupTableCounter += 1
}
dfjoin = unionDF
if(!(propsDQ.getProperty(DataQualityConstants.feedName).equalsIgnoreCase("SPP_claim"))){
  logger.info("DQM_INFO : Not for SPP. Removing DuplicateRecords")
  dfjoin = dfjoin.dropDuplicates()
}
//logger.info("DQM_INFO: Final Union DF Count "+dfjoin.count())
rddchange = dfjoin.rdd.map { x => x.mkString(delimeter) }
}
}
//logger.info("DQM_INFO: RDD Change Count "+rddchange.count())
}
else{
if (PatientLookup != null) {
if (PatientLookup.equalsIgnoreCase("Y")) {
val basePath = propsEnv.getProperty(EnvConstants.RAWBASEPATH)
for (lookupTableValues <- propsDQ.getProperty(DataQualityConstants.LOOKUPFILELOCATION).split(",")) {
  lookupCheckCount = lookupCheckCount + 1
val lookupTable = lookupTableValues.split(";")(0)
lookUpCheckMap(lookupTable) = lookupCheckCount
if (!lookupTableValues.split(";")(1).equalsIgnoreCase("sqoop")) {
lookupKeys = lookupTableValues.split(";").drop(1)
    parquetDf1 = spark.read.parquet(basePath + "/" + lookupTable)
} else {
lookupKeys = lookupTableValues.split(";").drop(2)
    //val hc = new HiveContext(sc)
    spark.sql("use " + sqoopedDatabase)
    parquetDf1 = spark.sql("select * from " + lookupTable)
}
  var joinKey: Column = null
var lookUpKeyForPickup = ""
var lookUpKeyForPickupAlias = ""

for (lookupKey <- lookupKeys) {
val firstlookupKey = lookupKey.split("\\=")(0)
val secondlookupKey = lookupKey.split("\\=")(1)
  if (joinKey == null) {
joinKey = upper(dfjoin(firstlookupKey)) === upper(parquetDf1(secondlookupKey))
    lookUpKeyForPickup = secondlookupKey
    //lookUpKeyForPickupAlias = lookupTable + ":" + firstlookupKey + ":" + secondlookupKey
        lookUpKeyForPickupAlias = lookupTable + ":" + secondlookupKey
  } else {
    joinKey = joinKey && upper(dfjoin(firstlookupKey)) === upper(parquetDf1(secondlookupKey))
  }
}
dfjoin = dfjoin.as("d1").join(parquetDf1.as("d2"), joinKey, "left_outer").select($"d1.*", parquetDf1(lookUpKeyForPickup) as lookUpKeyForPickupAlias)
}
if(!(propsDQ.getProperty(DataQualityConstants.feedName).equalsIgnoreCase("SPP_claim"))){
  dfjoin = dfjoin.dropDuplicates()
}
rddchange = dfjoin.rdd.map { x => x.mkString(delimeter) }
}
}
}



    var rawfile2_count = ParquetDf.count()

if (!fs.exists(new Path(Actualcountlocation))) {
val os = fs.create(new Path(Actualcountlocation))
val rawfile3_count = rawfile2_count.toString()
val countFileText = rawfile3_count + "|" + rawfile3_count + "|" + rawfile3_count
os.writeBytes(countFileText)
} else {

val hiscountRDD = sc.textFile(Actualcountlocation)

historyCount = hiscountRDD.collect()(0).split("\\|")(0).toLong
totalHistoryCount = hiscountRDD.collect()(0).split("\\|")(1).toLong
lastFullHistoryCount = hiscountRDD.collect()(0).split("\\|")(2).toLong

RowdiffRdd = historyCount - rawfile2_count
val dividentRdd = ((RowdiffRdd / rawfile2_count) * 100)
percentileRdd = dividentRdd.toString()
fs.delete(new Path(Actualcountlocation), true)
Thread.sleep(30 * 1000);
val os = fs.create(new Path(Actualcountlocation))
val rawfile4_count = rawfile2_count.toString()
// if (feedLoadType.equalsIgnoreCase("F")) {
if (loadTypeForCountCal.equalsIgnoreCase("F")) {
  val countFileText = rawfile4_count + "|" + rawfile4_count + "|" + rawfile4_count
  os.writeBytes(countFileText)
} else {
  val newTotalCount = rawfile2_count + totalHistoryCount
  val countFileText = rawfile4_count + "|" + newTotalCount.toString() + "|" + lastFullHistoryCount.toString()
  os.writeBytes(countFileText)
}
hisfilecountexists = true

}

    var Uniquecounter = 0
var BlankLinecounter = 0
var DuplicateRecordCounter = 0
def checkDQ(line: String): String = {
var fieldscount123: String = null
  var fieldscount: Array[String] = null
  Successflag = "failure"
  var expiryDate = "N/A"
  if (!isLineBlank(line)) {
var fieldscountTmp = line.split(splitString, -1)
    var validRecFlag = true
    errordesc = ""
    //Patient lookup check
    var uniquecounter = 0
    var lookUpCounter = 0
    var patientlookupcounter = 0
    if (PatientLookup != null) {
if (PatientLookup.equalsIgnoreCase("Y")) {
var lookUpCheckFailArray = ArrayBuffer[Boolean]()
    var lookUpCheckFailTables = ""

    for (lookUpChecks <- lookUpCheckMap) {
if (fieldscountTmp.reverse(lookupCheckCount - lookUpChecks._2).toLowerCase().equalsIgnoreCase("null") || fieldscountTmp.reverse(lookupCheckCount - lookUpChecks._2) == null || fieldscountTmp.reverse(lookupCheckCount - lookUpChecks._2).isEmpty()) {
  //AccArrayforLookupcols(lookUpCounter).add(1)
  validRecFlag = false
//lookUpCheckFailArray += true
lookUpCheckFailTables = lookUpCheckFailTables + "," + lookUpChecks._1
}

lookUpCounter += 1
    }
fieldscount = fieldscountTmp.dropRight(lookupCheckCount).mkString(delimeter).split(splitString, -1)
    fieldscount123 = fieldscountTmp.dropRight(lookupCheckCount).mkString(delimeter)
    if (!lookUpCheckFailTables.equalsIgnoreCase("")) {
UniqueBadrecAccum += 1
    errordesc = errordesc + "Failed: referential check failed for LookupTable :" + lookUpCheckFailTables
    }
} else {
fieldscount = fieldscountTmp
    fieldscount123 = fieldscountTmp.mkString(delimeter)
}
    } else {
fieldscount = fieldscountTmp
  fieldscount123 = fieldscountTmp.mkString(delimeter)
    }

if (!checkColCount(fieldscount.length)) {
  //Error Record --- Store as Bad Rec RDD
  columCountAccum.add(1)
  validRecFlag = false
  UniqueBadrecAccum += 1
  errordesc = "Failed:Col-Countcheck Actual: " + fieldscount.length + " Expected: " + getHeaderColCount
}

// check null
var nullCounter = 0
    if (nullcheckcols != null)
for (col <- nullcheckcols.split("\\,")) {
if (isNullorBlank(fieldscount(fieldIndexMap(col)))) {
  AccArrayforNull(nullCounter).add(1)
  validRecFlag = false
  UniqueBadrecAccum += 1
  errordesc = errordesc + "Failed:Null Check for col: " + col + " value is: null"

}
nullCounter += 1
}

//Check Numeric Columns
var numCounter = 0
    if (numcheckcols != null)
for (col <- numcheckcols.split("\\,")) {
if (!isNumber(fieldscount(fieldIndexMap(col)))) {
  AccArrayforNum(numCounter).add(1)
  validRecFlag = false
  UniqueBadrecAccum += 1
  errordesc = errordesc + "Failed:Num Check for col: " + col + " value is: " + fieldscount(fieldIndexMap(col))
}
numCounter += 1
}

//Check Date Columns
var dateCounter = 0
    if (datecheckcols != null) {
for (col <- datecheckcols.split(",")) {
val colArray = col.split("\\;")
    if (!checkDateFormat(fieldscount(fieldIndexMap(colArray(0).trim())), colArray(1))) {
//if (!checkDateFormat(col,dateFormat)) {
//BADDATERECFORCOL:col +=1
AccArrayforDate(dateCounter).add(1)
validRecFlag = false
UniqueBadrecAccum += 1
errordesc = errordesc + "Failed:Date Check for col: " + col + " value is: "
    }
dateCounter += 1
}
    }

/* var numericCounter = 0
        if (Nonnumeric != null)
          for (col <- Nonnumeric.split("\\,")) {
            if (!isaplhaNumeric(fieldscount(fieldIndexMap(col)))) {
              AccArrayforNonnumeric(numericCounter).add(1)
              validRecFlag = false
              errordesc = errordesc + "Failed:Numeric Check for col:" + col + " value is:" + fieldscount(fieldIndexMap(col))
            }
            numericCounter += 1
          }
 */
var validDateCounter = 0
    if (ValidDateCheck != null) {
for (col <- ValidDateCheck.split(",")) {
val colArray2 = col.split("\\;")

    if (!checkvalidDate(fieldscount(fieldIndexMap(colArray2(0).trim())), colArray2(1))) {
//if (!checkDateFormat(col,dateFormat)) {
//BADDATERECFORCOL:col +=1
//  AccArrayforDate(validDateCounter).add(1)
AccArrayForValidDate(validDateCounter).add(1)
validRecFlag = false
UniqueBadrecAccum += 1
errordesc = errordesc + "Failed :Valid Date Check for col:" + colArray2(0).trim() + "value is" + fieldscount(fieldIndexMap(colArray2(0).trim()))
    }
validDateCounter += 1
}
    }

var ValidValueCounter = 0
    if (ValidvalueCheck != null) {
for (col <- ValidvalueCheck.split(",")) {
val colArray = col.split("\\;")
    var valueArray = colArray
    valueArray = valueArray.drop(1)
    val recordValue = fieldscount(fieldIndexMap(colArray(0).trim()))
    var valueValid = false
    if (valueArray.contains(recordValue)) {
valueValid = true
    }
/*for (i <- 1 to colArray.length - 1) {
              if (recordValue == colArray(i).trim()) {
                valueValid += true
              } else {
                valueValid += false
              }
            }*/
if (!valueValid) {
  AccArrayforValidValue(ValidValueCounter).add(1)
  validRecFlag = false
  UniqueBadrecAccum += 1
  errordesc = errordesc + "Failed : Valid Value Check for col:" + colArray(0) + "value is" + fieldscount(fieldIndexMap(colArray(0).trim()))
}
}
ValidValueCounter += 1
    }

var ValidNonNumericCounter = 0
    if (Non_NumericCheck != null)
for (col <- Non_NumericCheck.split(",")) {
if (!nonNumerics(fieldscount(fieldIndexMap(col)))) {

  AccArrayForNonNumeric(ValidNonNumericCounter).add(1)
  validRecFlag = false
  UniqueBadrecAccum += 1
  errordesc = errordesc + "Failed : Valid Non-Numeric check for col:" + col + "value is:" + fieldscount(fieldIndexMap(col))
}
ValidNonNumericCounter += 1
}
var DateRangecounter = 0
    if (daterangecols != null)
for (col <- daterangecols.split("\\,")) {
if (!daterange(fieldscount(fieldIndexMap(col)))) {
  AccArrayforDateRangeCols(DateRangecounter).add(1)
  validRecFlag = false
  UniqueBadrecAccum += 1
  errordesc = errordesc + "Failed : Valid DateRange check for col:" + col + "value is:" + fieldscount(fieldIndexMap(col))
}
DateRangecounter += 1
}

if (deletionflag != null || deletionflag == "Y") {
  if (header.contains("DELETION_FLAG")) {

    expiryDate = "999912"
  }
}
if (validRecFlag == false) {
  // Uniquecounter+=1
  //UniqueBadrecAccum+=(1)
  toUniqueBadrecAccum += (1)
//Uniquecounter += 1

return fieldscount123 + delimeter + expiryDate + delimeter + errordesc + " BADRECTRUE"
} else {
  return fieldscount123 + delimeter + expiryDate + delimeter + "goodrecords"
}

  } else {
//Increment Blank Line Counter
blankLineAccum += (1)
    BlankLinecounter += 1
    UniqueBadrecAccum += 1
    toUniqueBadrecAccum += 1
    return fieldscount123 + delimeter + null + delimeter + "BlankLine: " + " BADRECTRUE"
  }

    }

    val dqStatsInfo = collection.mutable.ArrayBuffer[String]()

if (PatientLookup != null) {
if (PatientLookup.equalsIgnoreCase("Y")) {
processedRdd = rddchange.map(checkDQ)
} else {
processedRdd = rawfile.map(checkDQ)
}

} else {
processedRdd = rawfile.map(checkDQ)
}

    processedRdd.persist()
    logger.info("DQM_INFO: processedRdd Count "+processedRdd.count())

    /*val badRecRdd = processedRdd.filter { x => x.endsWith("BADRECTRUE") }
    val goodRecRdd = processedRdd.filter { x => x.endsWith("goodrecords") }

    //Filter Bad Records

    val badRecordCount = badRecRdd.count()*/
    //Filter Good Records

    //------BadRecord Percentage

    /*var badrecords = rawfile_count - goodRecordCount // minus\

    var badrecord = badrecords.toChar
    var percintile = badrecord.toFloat / rawfile_count.toChar*/ // division

    var AllRecRddTemp = processedRdd

    //    var Expriationflag = AllRecRddTemp.filter { x => x.endsWith("DeletionFlag") }

    //=============================================

        // Below will generate the composite Key Combination for Duplicate Check
        // Eg. (x.split(splitString)(fieldIndexMap(ATTENDEEID)),x.split(splitString)(fieldIndexMap(PROGRAMID)))
        def groupByKeyFormation (input: String,splitString : String,fieldIndexMap : scala.collection.mutable.Map[String,Int],dupCols : String) = {
                var compositeCol = ""
                for (individualCol <- dupCols.split("\\;")) {
                        if (compositeCol == "") {
                                compositeCol = input.split(splitString)(fieldIndexMap(individualCol)).replace("null","")
                        } else {
                                compositeCol = compositeCol + "," + input.split(splitString)(fieldIndexMap(individualCol)).replace("null","")
                        }
                }
                compositeCol = "("+compositeCol+")"
                compositeCol
        }
    var finalDuplicateRecRdd: RDD[String] = null
    var AllRecRdd1: RDD[String] = null

    var abc = ""
    var counter1 = 0
    var keys1 = ""
    if (duplicatecheckcols != null) {
//below will work only for one iteration when split by ','
for (col <- duplicatecheckcols.split("\\,")) {
var multipleFlag = false
var groupedRecords : org.apache.spark.rdd.RDD[(String, Iterable[String])] = null

if (col.contains(";")) {
println("Contains Composite Duplicate Check Config")
multipleFlag = true
//val mappedRecords = AllRecRddTemp.map { x => (groupByKeyFormation(x,splitString,fieldIndexMap,col),x)}
//mappedRecords.collect.take(5).foreach(println)
groupedRecords = AllRecRddTemp.map { x => (groupByKeyFormation(x,splitString,fieldIndexMap,col),x)}.groupByKey()
}
else
{
println("Contains one Duplicate Check Config")
groupedRecords = AllRecRddTemp.map { x => (x.split(splitString)(fieldIndexMap(col)), x) }.groupByKey()
}

var validFlage = false
//val groupedRecords = AllRecRddTemp.map { x => (x.split(splitString)(fieldIndexMap(col)), x) }.groupByKey()

var duplicateRecRdd = groupedRecords.filter({ case (key, value) => value.count { x => true } > 1 })
var nonDuplicateRecRdd = groupedRecords.filter({ case (key, value) => value.count { x => true } == 1 }).values.flatMap(i => i.toList)

finalDuplicateRecRdd = duplicateRecRdd.values.flatMap(i => i.toList).map { x =>
val fieldscount1 = x.split("\\|", -1)
val splitArr = x.split("\\|")

if (splitArr(splitArr.length - 1) == "goodrecords") {
DuplicateAccumulator += (1)
DuplicateRecordCounter += 1
UniqueBadrecAccum += 1
toUniqueBadrecAccum += 1
if (multipleFlag) {
splitArr(splitArr.length - 1) = " duplicate records on Composite column " + col + " BADRECTRUE"
} else {
//splitArr(splitArr.length - 1) = " duplicate records on column " + col + " with key: " + fieldscount1(fieldIndexMap(col)) + " BADRECTRUE"
splitArr(splitArr.length - 1) = " duplicate records on column " + col + " BADRECTRUE"
}
} else {
if (multipleFlag) {
splitArr(splitArr.length - 1) = splitArr(splitArr.length - 1) + " duplicate records on Composite column  " + col + " BADRECTRUE"
} else {
//splitArr(splitArr.length - 1) = splitArr(splitArr.length - 1) + " duplicate records on column  " + col + " with key: " + fieldscount1(fieldIndexMap(col)) + " BADRECTRUE"
splitArr(splitArr.length - 1) = splitArr(splitArr.length - 1) + " duplicate records on column  " + col + " BADRECTRUE"
}
}
splitArr.mkString(delimeter)
}
AllRecRdd1 = nonDuplicateRecRdd.union(finalDuplicateRecRdd)
  AllRecRddTemp = AllRecRdd1
}
    }
    

    val SchemaString = header + ",expiry_date,DQM_Status"
val schemaForMapping = SchemaString.split(",")
val schema = StructType(SchemaString.split(",").map { fieldName => StructField(fieldName, StringType, true) })

var rowRDD = AllRecRddTemp.map(_.split(splitString, -1)).map(x => x.toSeq).map { x => Row.fromSeq(x) }

    val partitioncol1 = "ingestion_dt_tm"
val partitioncol2 = "ingestion_dt_tm"
var feedname = propsDQ.getProperty(DataQualityConstants.feedName)
var finalDF = spark.createDataFrame(rowRDD, schema)
val nullConverter = udf((input: String) => {
if ((input.trim.length > 0) && !(input.equalsIgnoreCase("null"))) {
input.trim
} else null
})
finalDF = finalDF.select(finalDF.columns.map(c => nullConverter(col(c)).alias(c)): _*)

// ----------------- PARTIAL LOGIC IMPLEMENTATION

/* If load type is P, do following for implementing partial refresh in data:
 * 1. Fetching partialFrmDate and partialToDate from the .lst file.
 * 2. For the dates between partialFrmDate and partialToDate, hive query is written to retrieve date_month column.
 * 3. Will load that partition in Dataframe.
 * 4. For each partition will do following:
 *           a. Will go to HDFS dir path for that particular partition.
 *           b. Read all parquet files.
 *           c. Fetch transaction_date column index
 *           d. Loading the files from the affected partition to DF.
 *           e. Converting dates to desired format for comparison. Used UDF as well.
 *           f. If transaction_date lies between Refresh_From_Date and Refresh_To_Date, updating active_flag from "Y" to "N" in DataFrame
 *           g. Clean the affected partition path (Delete all files)
 *           h. Move all .parquet files from temp folder to affected partition
 *           i. Lastly delete temp folder
 */

var refAffectedPartPath = ""
var refAffectedPartPrePath = ""
val refDB = propsEnv.getProperty(EnvConstants.DATABASENAME2)
val refTableName = propsDQ.getProperty(CommonConstants.RFDTBLNAME).replace("SET hivevar:ptbl=", "")
val transactionDateColName = propsDQ.getProperty(CommonConstants.TRANSACTIONDATECOLNAME)
val transactionDateFormat = propsDQ.getProperty(CommonConstants.TRANSACTIONDATEFORMAT)
val RefinedBasePathStr = propsEnv.getProperty(EnvConstants.REFINEDBASEPATH)

var partialRefreshPartitions = ArrayBuffer[String]()

if (loadType.trim() == "P") {
 val oldfeedmetadata = spark.read.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/" + feedname + "_refinedmetadata.txt").take(1).mkString("|")

        if (oldfeedmetadata.isEmpty || oldfeedmetadata == null) {
          logger.error("No lines found in the feedmetadata file ..")
          throw new RuntimeException("No lines found in the feedmetadata file ..")
        }
        val feedMetadataArr = oldfeedmetadata.split('|')
        // val date_month = feedMetadataArr(0)
        val ingestion_dt_tm = feedMetadataArr(1) 
// Fetching affected Partition
//val refPartitionCol = "select distinct ingestion_dt_tm from " + refDB + ". " + refTableName + " where " + transactionDateColName + " >= '" + partialFrmDate + "' and " + transactionDateColName + " <= '" + partialToDate + "'"
println("select distinct ingestion_dt_tm from " + refDB + ". " + refTableName + " where " + transactionDateColName + " >= '" + partialFrmDate + "' and " + transactionDateColName + " <= '" + partialToDate + "' and ingestion_dt_tm='"+ingestion_dt_tm.split('=')(1)+"'") 
//val hqlCon = new HiveContext(sc)
        val refPartitionCol = "select distinct ingestion_dt_tm from " + refDB + ". " + refTableName + " where " + transactionDateColName + " >= '" + partialFrmDate + "' and " + transactionDateColName + " <= '" + partialToDate + "' and ingestion_dt_tm='"+ingestion_dt_tm.split('=')(1)+"'" 
//val hqlCon = new HiveContext(sc)
val refAffectedPartColDF = spark.sql(refPartitionCol)
// Looping all partitions, in case of multiple partition
refAffectedPartColDF.collect().foreach { x =>
refAffectedPartPath = RefinedBasePathStr + "/" + feedname + "/ingestion_dt_tm=" + x(0) + "/"
refAffectedPartPrePath = propsEnv.getProperty(EnvConstants.PREREFINEDBASEPATH) + "/" + feedname + "/ingestion_dt_tm=" + x(0)
partialRefreshPartitions += x(0).toString()
val refAffectedPartDF = spark.read.parquet(refAffectedPartPath)

// Converting date format of Refresh_From_Date and Refresh_To_Date to desired format(yyyyMMdd), for comparison
val inptDateFmt: SimpleDateFormat = new SimpleDateFormat(transactionDateFormat)

var refUpdatedDF: DataFrame = null
if (transactionDateFormat.contains("MMM")) {
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
  if (!(inCol1 == "" || inCol1 == null || inCol1.isEmpty() || inCol1 == "null" || inCol1 == "NULL")) {
    try {
val inCol1Parsed = inptDateFmt.parse(inCol1)
  outputDateFmt.format(inCol1Parsed)
    } catch {
    case t: ParseException => { "" } // TODO: handle error
    }
  } else {
    ""
  }
  }

  // Updating active_flag from "Y" to "N" in DataFrame; if transaction_date lies between Refresh_From_Date and Refresh_To_Date
  //        val refUpdatedDF = refAffectedPartDF.withColumn(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME), when(refAffectedPartDF(transactionDateColName).isNotNull and (refAffectedPartDF(transactionDateColName) !== "") and dateFormatterUDF(refAffectedPartDF(transactionDateColName)) >= partialFrmDateValue and dateFormatterUDF(refAffectedPartDF(transactionDateColName)) <= partialToDateValue, "N").otherwise("Y"))

//  refUpdatedDF = refAffectedPartDF.withColumn(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
//  when(refAffectedPartDF(transactionDateColName).isNull and (refAffectedPartDF(transactionDateColName) === ""), "Y")
//  .otherwise(when(dateFormatterUDF(refAffectedPartDF(transactionDateColName)) >= partialFrmDateValue
//  and dateFormatterUDF(refAffectedPartDF(transactionDateColName)) <= partialToDateValue, "N").otherwise("Y")))
//} else {
//  refUpdatedDF = refAffectedPartDF.withColumn(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
//  when(refAffectedPartDF(transactionDateColName).isNull and (refAffectedPartDF(transactionDateColName) === ""), "Y")
//  .otherwise(when(refAffectedPartDF(transactionDateColName) >= partialFrmDate
//  and refAffectedPartDF(transactionDateColName) <= partialToDate, "N").otherwise("Y")))
//}
  
  refUpdatedDF = refAffectedPartDF.withColumn(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
  when(refAffectedPartDF(transactionDateColName).isNull and (refAffectedPartDF(transactionDateColName) === ""), "Y")
  .otherwise(when(dateFormatterUDF(refAffectedPartDF(transactionDateColName)) >= partialFrmDateValue
  and dateFormatterUDF(refAffectedPartDF(transactionDateColName)) <= partialToDateValue
  and (refAffectedPartDF(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME)) === "Y"), "N").otherwise("Y")))
} else {
  refUpdatedDF = refAffectedPartDF.withColumn(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME),
  when(refAffectedPartDF(transactionDateColName).isNull and (refAffectedPartDF(transactionDateColName) === ""), "Y")
  .otherwise(when(refAffectedPartDF(transactionDateColName) >= partialFrmDate
  and refAffectedPartDF(transactionDateColName) <= partialToDate
  and (refAffectedPartDF(propsDQ.getProperty(CommonConstants.ACTIVEFLAGCOLNAME)) === "Y"), "N").otherwise("Y")))
} 

val refAffectedPartPathTemp = propsEnv.getProperty(EnvConstants.PREREFINEDBASEPATH) + "/" + feedname + "/temp"
val refFilesToMove = refAffectedPartPathTemp + "/*.parquet"

//val refAffectedPartPathCleanUp = refAffectedPartPath + "*"

refUpdatedDF = refUpdatedDF.select(schemaForMapping.head, schemaForMapping.tail: _*)
if (changedSchemardd.count() > 0) {
  for (changedSchema <- masterSchemaColsArr2) {
    if (refUpdatedDF.columns.contains(changedSchema)) {
val intermeditaeDF = refUpdatedDF.withColumn("ChangedColumn", refUpdatedDF(changedSchema))
  refUpdatedDF = intermeditaeDF.drop(changedSchema).withColumnRenamed("ChangedColumn", changedSchema)
    }
  }
}
//refUpdatedDF.write.parquet(refAffectedPartPathTemp)
refUpdatedDF.write.parquet(refAffectedPartPrePath)

//Seq("hadoop", "fs", "-rm", refAffectedPartPathCleanUp).!
//Seq("hadoop", "fs", "-mv", refFilesToMove, refAffectedPartPrePath).!
//Seq("hadoop", "fs", "-rm", "-r", refAffectedPartPathTemp).!

Successflag = "Success"
}

}

    // -------- END OF PARTIAL REFRESH LOGIC

    val outputDirectory = propsEnv.getProperty(EnvConstants.PREREFINEDBASEPATH) + "/" + feedname + "/" + partitioncol1 + "=" + ingestion_dt + "/"

var finalWriteUpDF = finalDF.select(schemaForMapping.head, schemaForMapping.tail: _*)
if (changedSchemardd.count() > 0) {
for (changedSchema <- masterSchemaColsArr2) {
if (finalWriteUpDF.columns.contains(changedSchema)) {
val intermeditaeDF = finalWriteUpDF.withColumn("ChangedColumn", finalWriteUpDF(changedSchema))
finalWriteUpDF = intermeditaeDF.drop(changedSchema).withColumnRenamed("ChangedColumn", changedSchema)
}
}
}
    //peopleFinalDF.write.option("delimeter", "\\|") save (outputDirectory)
    val df1 = finalWriteUpDF.filter(finalWriteUpDF("DQM_Status")==="goodrecords")
df1.cache()
val goodRecordsCount = df1.count()

//*********************************************************************************************************************************
val refinedPartitionColumns = propsDQ.getProperty(CommonConstants.RFDPARTITIONCOL).split("=")(1)
logger.info("refinedPartitionColumns values : " + refinedPartitionColumns)
val PREREFINEDBASEPATH = propsEnv.getProperty(EnvConstants.PREREFINEDBASEPATH)

if (refinedPartitionColumns.split(",").length == 1 )
{
partitionColumn1 = refinedPartitionColumns.split(",")(0).split(" ")(0)
//partitionColumn1 = "ingestion_dt_tm"
val writepath1 = PREREFINEDBASEPATH + "/" + feedname + "/" + partitionColumn1.toLowerCase() + "=" + ingestion_dt
logger.info("Refined Partition Writepath1 : " + writepath1)
finalWriteUpDF.write.parquet(writepath1)
}
else if(refinedPartitionColumns.split(",").length == 3) {

partitionColumn1 = refinedPartitionColumns.split(",")(0).split(" ")(0)
partitionColumn2 = refinedPartitionColumns.split(",")(1).split(" ")(0)
partitionColumn3 = refinedPartitionColumns.split(",")(2).split(" ")(0)

logger.info("2 Partitions have been specified")
logger.info("Partition Column1 value is : " + partitionColumn1)
logger.info("Partition Column2 value is : " + partitionColumn2)
logger.info("Partition Column3 value is : " + partitionColumn3)

val writepath2 = PREREFINEDBASEPATH + "/" + feedname + "/" + partitionColumn1.toLowerCase() + "=" + ingestion_dt
logger.info("Refined Partition Writepath2 : " + writepath2)
//finalWriteUpDF.write.parquet(writepath2)

if(finalWriteUpDF.columns.contains(partitionColumn2))
{
  //var DF1 = finalWriteUpDF.groupBy(partitionColumn1)
  logger.info("Partition Column 2 is present in the File")

  partitionValues = finalWriteUpDF.select(partitionColumn2).distinct.collect.flatMap(_.toSeq)
  val part1Array = partitionValues.map(PartitionValue => finalWriteUpDF.where($"$partitionColumn2" <=> PartitionValue))

  for (DF1 <- part1Array)
  {
val partitionValue2 = DF1.select(partitionColumn2).first().toString().replace("[", "").replace("]", "")
    val writepath3 = writepath2 + "/" + partitionColumn2.toLowerCase() + "=" + partitionValue2
    logger.info("Refined Primary Partition Writepath : " + writepath3)

    //DF.write.parquet(PREREFINEDBASEPATH + "/" + feedname + "/" + partitionValue1)
    if(DF1.columns.contains(partitionColumn3))
    {
logger.info("Both Partition Column 2 and Partition Column 3 are present in the File")
val subPartitionValues = DF1.select(partitionColumn3).distinct.collect.flatMap(_.toSeq)
val part2Array = subPartitionValues.map(subPartitionValue => DF1.where($"$partitionColumn3" <=> subPartitionValue))
for (DF2 <- part2Array)
{
val partitionValue3 = DF2.select(partitionColumn3).first().toString().replace("[", "").replace("]", "")
    DF2.drop(partitionColumn2).drop(partitionColumn3).write.parquet(writepath3 + "/" + partitionColumn3.toLowerCase() + "=" + partitionValue3)
    logger.info("Refined Third Partition Writepath : " + writepath3 + "/" + partitionColumn3.toLowerCase() + "=" + partitionValue3)
}
    }
    else{
DF1.drop(partitionColumn2).write.parquet(writepath3)
    }
  }
}
else if(finalWriteUpDF.columns.contains(partitionColumn3))
{
  logger.info("Only Partition Column 3 is present in the File")
  val writepath4 = PREREFINEDBASEPATH + "/" + feedname + "/" + partitionColumn1.toLowerCase() + "=" + ingestion_dt
  logger.info("Refined Primary Partition Writepath : " + writepath4)

  val subPartitionValues = finalWriteUpDF.select(partitionColumn3).distinct.collect.flatMap(_.toSeq)
  val part2Array = subPartitionValues.map(subPartitionValue => finalWriteUpDF.where($"$partitionColumn3" <=> subPartitionValue))

  for (DF1 <- part2Array)
  {
val partitionValue3 = DF1.select(partitionColumn3).first().toString().replace("[", "").replace("]", "")
    DF1.drop(partitionColumn3).write.parquet(writepath4 + "/" + partitionColumn3.toLowerCase() + "=" + partitionValue3)
    logger.info("Refined Partition 3 Writepath : " + writepath4 + "/" + partitionColumn3.toLowerCase() + "=" + partitionValue3)
  }
}
else{
  logger.info("No Partition Column is present in the File")
  //partitionColumn1 = "ingestion_dt_tm"
  val writepath5 = PREREFINEDBASEPATH + "/" + feedname + "/" + partitionColumn1.toLowerCase() + "=" + ingestion_dt
  logger.info("Refined Partition Writepath : " + writepath5)
  finalWriteUpDF.write.parquet(writepath5)
}
}

else if(refinedPartitionColumns.split(",").length == 2) {
partitionColumn1 = refinedPartitionColumns.split(",")(0).split(" ")(0)
partitionColumn2 = refinedPartitionColumns.split(",")(1).split(" ")(0)
logger.info("2 Partition has been specified")
logger.info("Partition Column2 value is : " + partitionColumn2)
val writepath6 = PREREFINEDBASEPATH + "/" + feedname + "/" + partitionColumn1.toLowerCase() + "=" + ingestion_dt
//finalWriteUpDF.write.parquet(writepath2)
if(finalWriteUpDF.columns.contains(partitionColumn2))
{
  logger.info("Partition Column 2 is present in the File")
  partitionValues = finalWriteUpDF.select(partitionColumn2).distinct.collect.flatMap(_.toSeq)
  val part1Array = partitionValues.map(PartitionValue => finalWriteUpDF.where($"$partitionColumn2" <=> PartitionValue))

  for (DF3 <- part1Array)
  {
val partitionValue2 = DF3.select(partitionColumn2).first().toString().replace("[", "").replace("]", "")
    val writepath7 = writepath6 + "/" + partitionColumn2.toLowerCase() + "=" + partitionValue2
    DF3.drop(partitionColumn2).write.parquet(writepath7)
    logger.info("Refined Primary Partition Writepath : " + writepath7)
  }

}
else
{
  logger.info("Partition Column 2 is not present in the File")
  finalWriteUpDF.write.parquet(writepath6)
  logger.info("Refined Primary Partition Writepath : " + writepath6)

}


}
else{
partitionColumn1 = refinedPartitionColumns.split(",")(0).split(" ")(0)
//partitionColumn1 = "ingestion_dt_tm"
val writepath1 = PREREFINEDBASEPATH + "/" + feedname + "/" + partitionColumn1.toLowerCase() + "=" + ingestion_dt
logger.info("Refined Partition Writepath1 : " + writepath1)
finalWriteUpDF.write.parquet(writepath1)

}

    //finalWriteUpDF.write.parquet(outputDirectory)
    Successflag = "Success"

/* Code start to handle Approval Logic*/
var partitionMetaInfo = loadType + "|" + partitioncol1 + "=" + ingestion_dt
if (loadType.trim() == "P") {
partitionMetaInfo = partitionMetaInfo + "|"
for (partitionscol2 <- partialRefreshPartitions) {
  partitionMetaInfo = partitionMetaInfo + partitioncol1 + "=" + partitionscol2 + ","
}
if (partitionMetaInfo.endsWith(",")) {
partitionMetaInfo.dropRight(1)
}
}
    val os = fs.create(new Path(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/" + feedname + "_refinedmetadata.txt"))
os.writeBytes(partitionMetaInfo)

/* Code end to handle Approval Logic*/

var enddate = df.format(Calendar.getInstance().getTime()).toString();
    var StatsEndTime = Starttimeformat.format(Calendar.getInstance().getTime())
val uniqueBadrecCount= rawfile_count - goodRecordsCount

val badrecordpercintile = (uniqueBadrecCount / rawfile_count.toFloat) * 100

val filePath = propsDQ.getProperty(CommonConstants.AUDITFILEPATH) /*+"/"+propsDQ.getProperty(DataQualityConstants.AuditfileLocation)*/

val outFile = new Path(filePath)

var outStream: FSDataOutputStream = null
if (!fs.exists(outFile)) {
outStream = fs.create(outFile)

} else {
outStream = fs.append(outFile)

}

val bufWriter = new BufferedWriter(new OutputStreamWriter(outStream))

val fileSize = fs.getContentSummary(outFile).getLength()
val job_name = "DATAQUALITY"
val batchId = sc.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/loadmetadata.txt").take(1)(0).split("\\,")(0)
val dataSet = sc.textFile(propsEnv.getProperty(CommonConstants.METADATALOCATION) + "/loadmetadata.txt").take(1)(0).split("\\,")(3)

bufWriter.newLine()
bufWriter.append(batchId + "|" + dataSetFromEnv + "|" + vendorFromEnv + "|" + Market_CD + "|" + feedname + "|" + job_name + "|" + Statstartime.toString() + "|" + StatsEndTime.toString() + "|" + rawfile_count + "|" + Successflag + "|" /*+badrecordpercintile*/ )

bufWriter.close()
outStream.close()

var totalCountStats = ""
var historyCountStats = ""
var totalHistoryCountStats = ""
var RowdiffRddStats = ""
var percentileRddStats = ""
var duplicatecheckcolsStats = ""
var datasetIdentifier = ""
var loadTypeIdentifier = ""
val nullcheckcolsInfo = collection.mutable.ArrayBuffer[String]()
val numcheckcolsInfo = collection.mutable.ArrayBuffer[String]()
val datecheckcolsInfo = collection.mutable.ArrayBuffer[String]()
val NonnumericInfo = collection.mutable.ArrayBuffer[String]()
val ValidDateCheckInfo = collection.mutable.ArrayBuffer[String]()
val ValidvalueCheckInfo = collection.mutable.ArrayBuffer[String]()
val Non_NumericCheckInfo = collection.mutable.ArrayBuffer[String]()
val LookupcolsInfo = collection.mutable.ArrayBuffer[String]()
val daterangecolsInfo = collection.mutable.ArrayBuffer[String]()
var duplicateCheckCol = ""
if (dataSet.equalsIgnoreCase("Detailed_Ambassador_Graduation") || dataSet.equalsIgnoreCase("Detailed_Ambassador_Transactions") || dataSet.equalsIgnoreCase("CRM_Kit_Delivery_Transactions")
  || dataSet.equalsIgnoreCase("Patient_Support_Program_Dataset") || dataSet.equalsIgnoreCase("Copay_Claims") || dataSet.equalsIgnoreCase("OPUS_Pharmacy_Dimension")
  || dataSet.equalsIgnoreCase("SPP_Pharmacy_Dimension") || dataSet.equalsIgnoreCase("Humira_PS_Patient_Referrals") || dataSet.equalsIgnoreCase("Humira_SPP_Patient_Referrals")) {
datasetIdentifier = "SHA Integrated"
} else if (dataSet.equalsIgnoreCase("Longitudinal")) {
datasetIdentifier = "Epsilon Longitudinal"
} else if (dataSet.equalsIgnoreCase("PTD")) {
datasetIdentifier = "SHA PTD"
} else {
if (dataSet.isEmpty() || dataSet.equalsIgnoreCase("") || dataSet.equalsIgnoreCase(" ")) {
  datasetIdentifier = dataSetFromEnv.replaceAll("_", " ")
} else {
  datasetIdentifier = dataSet.replaceAll("_", " ")
}
}
if (feedLoadType.equalsIgnoreCase("F")) {
loadTypeIdentifier = "Full"
} else {
loadTypeIdentifier = "Incremental"
}

if (hisfilecountexists) {
historyCountStats = historyCount.toString()
totalHistoryCountStats = totalHistoryCount.toString()
RowdiffRddStats = RowdiffRdd.toString()
percentileRddStats = percentileRdd.toString()
}
//  if (feedLoadType.equalsIgnoreCase("F")) {
if (loadTypeForCountCal.equalsIgnoreCase("F")) {
totalCountStats = rawfile2_count.toString()
} else {
if (hisfilecountexists) {
totalCountStats = (rawfile2_count + totalHistoryCount).toString()
} else {
totalCountStats = rawfile2_count.toString()
}

}
if (duplicatecheckcols != null) {
duplicatecheckcolsStats = finalDuplicateRecRdd.count.toString()
duplicateCheckCol = duplicatecheckcols.split(",")(0)
}
var totalCurrentCount: Long = 0
var totalFinalHistoryCount: Long = 0
if (feedLoadType.equals("F")) {
totalCurrentCount = rawfile_count
totalFinalHistoryCount = 0
} else {
totalCurrentCount = totalHistoryCount + rawfile_count
totalFinalHistoryCount = 0
}

val feedNameDQM = propsDQ.getProperty(DataQualityConstants.feedName).toLowerCase().replace("hc_", "").replace("humira_", "").replace("_", " ").toUpperCase()
var metricsInfo = batchId + "|" + feedNameDQM + "|" + starttime + "|" + startYear + "|" + startMonth + "|" + rawfile_count + "|" + totalCountStats + "|" + blankLineAccum.value + "|" + columCountAccum.value + "|" + historyCount + "|" + totalHistoryCountStats + "|" + RowdiffRdd + "|" + percentileRdd + "|" + uniqueBadrecCount + "|" + badrecordpercintile + "|" + duplicateCheckCol + "|" + duplicatecheckcolsStats
if (AccArrayforNull != null) {
  for (accVal <- 0 to AccArrayforNull.length - 1) {
nullcheckcolsInfo += "|" + nullcheckcols.split(",")(accVal) + "|" + AccArrayforNull(accVal).value
  }
} else {
  nullcheckcolsInfo += "||"
}

if (AccArrayforNum != null) {
for (accVal <- 0 to AccArrayforNum.length - 1) {
numcheckcolsInfo += "|" + numcheckcols.split(",")(accVal) + "|" + AccArrayforNum(accVal).value
}
} else {
numcheckcolsInfo += "||"
}

if (AccArrayforDate != null) {
for (accVal <- 0 to AccArrayforDate.length - 1) {
datecheckcolsInfo += "|" + datecheckcols.split(",")(accVal).split("\\;")(0) + "|" + AccArrayforDate(accVal).value
}
} else {
datecheckcolsInfo += "||"
}
if (AccArrayforNonnumeric != null) {
for (accVal <- 0 to AccArrayforNonnumeric.length - 1) {
NonnumericInfo += "|" + Nonnumeric.split(",")(accVal) + "|" + AccArrayforNonnumeric(accVal).value
}
} else {
NonnumericInfo += "||"
}
if (AccArrayForValidDate != null) {
for (accval <- 0 to AccArrayForValidDate.length - 1) {
ValidDateCheckInfo += "|" + ValidDateCheck.split(",")(accval).split("\\;")(0) + "|" + AccArrayForValidDate(accval).value
}
} else {
ValidDateCheckInfo += "||"
}

if (AccArrayforValidValue != null) {
for (accval <- 0 to AccArrayforValidValue.length - 1) {
ValidvalueCheckInfo += "|" + ValidvalueCheck.split(",")(accval).split("\\;")(0) + "|" + AccArrayforValidValue(accval).value
}
} else {
ValidvalueCheckInfo += "||"
}

if (AccArrayForNonNumeric != null) {
for (accval <- 0 to AccArrayForNonNumeric.length - 1) {
Non_NumericCheckInfo += "|" + Non_NumericCheck.split(",")(accval) + "|" + AccArrayForNonNumeric(accval).value
}
} else {
Non_NumericCheckInfo += "||"
}

if (Lookupcols != null) {
var lookUpColFailCountMap = Map[String, Long]()

  for (lCol <- Lookupcols.split(",").par) {
var lcol1 = ""
    if (lCol.contains("(")) {
    if(loadNullAsGood.equalsIgnoreCase("Y")){
    var lcol2 = lCol.replace("(", "").replace(")", "")
                                        lcol1 = lcol2.split(":")(0).trim() + ":" + lcol2.split(":")(1).trim() + ":" + lcol2.split(":")(2).trim()
    } else {
    lcol1 = lCol.split(":")(0) + ":" + lCol.split(":")(1).replace("(", "").replace(")", "").split(":")(0).trim() + ":" + lCol.split(":")(1).replace("(", "").replace(")", "").split(":")(1).trim()
    }
    } else {
lcol1 = lCol
    }
                                    //var index = dfjoin.columns.indexOf(lcol1)
//                                  lookUpColFailCountMap(lCol) = dfjoin.rdd.filter(_.isNullAt(index)).count()

lookUpColFailCountMap(lCol) = dfjoin.filter(dfjoin(lcol1).isNull).count()
    //lookUpColFailCountMap += (lCol -> dfjoin.filter(dfjoin(lcol1).isNull).count().toLong)
  }

for (lColumn <- Lookupcols.split(",")) {
LookupcolsInfo += "|" + lColumn + "|" + lookUpColFailCountMap(lColumn)

}
} else {
LookupcolsInfo += "||"

}

if (AccArrayforDateRangeCols != null) {
for (accval <- 0 to AccArrayforDateRangeCols.length - 1) {
daterangecolsInfo += "|" + daterangecols.split(",")(accval) + "|" + AccArrayforDateRangeCols(accval).value
}
} else {
daterangecolsInfo += "||"
}

var finalStatsInfo1 = metricsInfo + nullcheckcolsInfo(0) + numcheckcolsInfo(0) + datecheckcolsInfo(0) + NonnumericInfo(0) + ValidDateCheckInfo(0) + ValidvalueCheckInfo(0) + Non_NumericCheckInfo(0) + LookupcolsInfo(0) + daterangecolsInfo(0) + "|" + enddate + "|" + Market_CD + "|" + loadTypeIdentifier + "|" + datasetIdentifier
dqStatsInfo += finalStatsInfo1
var countList: Array[Int] = Array(nullcheckcolsInfo.length, numcheckcolsInfo.length, datecheckcolsInfo.length, NonnumericInfo.length, ValidDateCheckInfo.length, ValidvalueCheckInfo.length, Non_NumericCheckInfo.length, LookupcolsInfo.length, daterangecolsInfo.length)

val maxCount = minMax(countList)
for (runCount <- 1 to (maxCount._2 - 1)) {
  var nullInfo = "||"
  var numInfo = "||"
  var dateInfo = "||"
  var NonnumInfo = "||"
  var ValidDateInfo = "||"
  var ValidvaluInfo = "||"
  var Non_NumericInfo = "||"
  var LookupInfo = "||"
  var daterangeInfo = "||"
  if (nullcheckcolsInfo != null) {
    if (nullcheckcolsInfo.length > runCount) {
nullInfo = nullcheckcolsInfo(runCount)
    }
  }
  if (numcheckcolsInfo != null) {
if (numcheckcolsInfo.length > runCount) {
  numInfo = numcheckcolsInfo(runCount)
}
  }
  if (datecheckcolsInfo != null) {
if (datecheckcolsInfo.length > runCount) {
  dateInfo = datecheckcolsInfo(runCount)
}
  }
  if (NonnumericInfo != null) {
if (NonnumericInfo.length > runCount) {
  NonnumInfo = NonnumericInfo(runCount)
}
  }
  if (ValidDateCheckInfo != null) {
if (ValidDateCheckInfo.length > runCount) {
  ValidDateInfo = ValidDateCheckInfo(runCount)
}
  }
  if (ValidvalueCheckInfo != null) {
if (ValidvalueCheckInfo.length > runCount) {
  ValidvaluInfo = ValidvalueCheckInfo(runCount)
}
  }
  if (Non_NumericCheckInfo != null) {
if (Non_NumericCheckInfo.length > runCount) {
  Non_NumericInfo = Non_NumericCheckInfo(runCount)
}
  }
  if (LookupcolsInfo != null) {
if (LookupcolsInfo.length > runCount) {
  LookupInfo = LookupcolsInfo(runCount)
}
  }
  if (daterangecolsInfo != null) {
if (daterangecolsInfo.length > runCount) {
  daterangeInfo = daterangecolsInfo(runCount)
}
  }
  var finalStatsInfo2 = metricsInfo + nullInfo + numInfo + dateInfo + NonnumInfo + ValidDateInfo + ValidvaluInfo + Non_NumericInfo + LookupInfo + daterangeInfo + "|" + enddate + "|" + Market_CD + "|" + loadTypeIdentifier + "|" + datasetIdentifier
  dqStatsInfo += finalStatsInfo2
}
val outFileMetric = new Path(statsOutputPath + "/DQMetric.txt")
var newFileFlag = true
var outStreamMetric: FSDataOutputStream = null
if (!fs.exists(outFileMetric)) {
  outStreamMetric = fs.create(outFileMetric)

} else {
  outStreamMetric = fs.append(outFileMetric)
  newFileFlag = false
}
var metricsOutput = dqStatsInfo.mkString("\n")
val bufWriterMetric = new BufferedWriter(new OutputStreamWriter(outStreamMetric))
if (!newFileFlag) {
  bufWriterMetric.newLine()
}
bufWriterMetric.append(metricsOutput)

bufWriterMetric.close()
outStreamMetric.close()

processedRdd.unpersist(true)
unionDF.unpersist(true)

  }
}