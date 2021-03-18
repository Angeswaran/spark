package com.humira.dataingestion

/**
 * *****************************************************************************************
 *  @Author By CTS BIGDATA TEAM
 *  @Version 2.10.5
 *  @Description: Following steps are included in this class:
 *         1. Database Creation on Hive if not exists.
 *         2. Creating three external tables in RAW layer:
 *                            a. On top of source data
 *                            b. Table with P1 as suffix: On second run, first run data will be moved here.
 *                            c. Table with P2 as suffix: On third run, second run data will be moved here.
 *                            This way we can store last three months data. P1 & P2 table will be created only when file exists.
 *         3. Adding partitions while creating table: DATE_MONTH
 *         4. Alter table command to add new added columns : It will run only when schema is changing.
 *         5. Command to run MSCK REPAIR to update metastore for newly added partitions.
 *         6. Creating AUDIT table to load record counts, status, error_messages etc. of the running job in RAW, DQ & REFINED layer.
 *         7. Added STARTTIME and ENDTIME of the running job for the particular feed.
 *
 * **************************************************************************************
 */

import java.io._
import java.util.{ Calendar, Properties }

import com.humira.constants.{ CommonConstants, EnvConstants, DataQualityConstants }
import com.humira.utilities.CustomLogger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.hive.spark
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._
import scala.io.Source
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.SparkConf
import java.nio.file.{ Paths, Files }
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.tools.ant.sabbus.Break
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FSDataOutputStream

/*class RawLayer(
    val props: Properties,
    val spark: spark,
    val sparkContext: SparkContext,
    val envProps: Properties) {*/

class RawLayer() {

  /*    def readEnvProperties(propertyFilePath :String){
    envProps = new ReadPropeties().readproperty(propertyFilePath)
  }*/

  def readEnvProperties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)

  }

  def readProperties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)

  }
  val spark = SparkSession.builder().appName("RawLayer")
              .config("hive.exec.dynamic.partition", "true")
              .config("hive.exec.dynamic.partition.mode", "nonstrict")
              .getOrCreate() 
  //var sparkConf = new SparkConf().setAppName("RawLayer")
  val sparkContext = spark.sparkContext
  //var sparkContext = new SparkContext(sparkConf)
  //var spark = new spark(sparkContext)
  //spark.setConf("hive.exec.dynamic.partition", "true")
  //spark.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  val conf = sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)

  var exceptionEncountered: Boolean = false

  val logger = new CustomLogger().getLogger(this.getClass().toString())

  def main(args: Array[String]) {

    var props: Properties = readProperties(args(0))
    var envProps: Properties = readEnvProperties(args(2))

    val df = new SimpleDateFormat("HH:mm:ss");
    val calobj = Calendar.getInstance();
    val starttime = df.format(calobj.getTime());
    val listFilePath = envProps.getProperty(CommonConstants.FILELISTCURRENTABSOLUTEPATH) + "/" + props.getProperty(CommonConstants.FILELISTNAME)
    val listFileRDD = sparkContext.textFile(listFilePath)
    val listFileArray = listFileRDD.collect() //collect RDD into an Array
    var splitString = "\\"+props.getProperty(CommonConstants.RAWTBLDELIMITER)
    val isStringQualifierDoubleQuotes = props.getProperty(CommonConstants.ISSTRINGQUALIFIERDOUBLEQUOTES)
   
    if(!(isStringQualifierDoubleQuotes.equalsIgnoreCase("Y"))){
     splitString = splitString + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    }
    
    var FullLoadExists = ""
    val Market_CD = envProps.getProperty(CommonConstants.MARKETCD)
    FullLoadExists = listFileArray(0).split(splitString)(4)
    val p1p2CreateFlag = props.getProperty(CommonConstants.P1P2CREATEFLAG)
    
    val p1 = props.getProperty(CommonConstants.RAWTBLLOCATION_P1).split("=")
    val p2 = props.getProperty(CommonConstants.RAWTBLLOCATION_P2).split("=")

    val infilePathP1 = envProps.getProperty(EnvConstants.RAWBASEPATH) + CommonConstants.FILE_SEPARATOR + p1(1)
    val infilePathP2 = envProps.getProperty(EnvConstants.RAWBASEPATH) + CommonConstants.FILE_SEPARATOR + p2(1)

    val inFileP1 = new Path(infilePathP1)
    val inFileP2 = new Path(infilePathP2)

    var rawStatus: Boolean = true
    var auditStatus: Boolean = true
    var errorMessage = ""
    var tempTblLocPath = ""

    val feedFrmPropFile = props.getProperty(CommonConstants.FEEDFROMPROPFILE).toUpperCase()
    val feedmetadata = sparkContext.textFile(envProps.getProperty(CommonConstants.METADATALOCATION) + "/" + props.getProperty(DataQualityConstants.FEEDMETADATALOC)).take(1).mkString(",")
    val feedMetadataArr = feedmetadata.split(",")
    val date_month = feedMetadataArr(0)
    
    val dbName1 = envProps.getProperty(EnvConstants.DATABASENAME1)
    var schema_table = Array[String]()
    var schema_table_P1 = Array[String]()
    var schema_table_P2 = Array[String]()
    val incomingSchemaFile = props.getProperty(CommonConstants.RAWNEWSCHEMALOCATION)
    val feedname = props.getProperty(CommonConstants.MASTERSCHEMAFILENAME).replace(".txt", "")
    val schemaChangeFilePath = envProps.getProperty(EnvConstants.STRINGCHANGEDSCHEMAPATH) + "/" + feedname + "/" + incomingSchemaFile
    val schemaChangeFile = new Path(schemaChangeFilePath)
    val tableLocation = props.getProperty(CommonConstants.RAWTBLLOCATION).split("=")
    /*  def setHiveVars() = {*/
    try {      
      spark.sql(props.getProperty(CommonConstants.COMMONVAR2) + dbName1)
      val dbName4 = envProps.getProperty(EnvConstants.DATABASENAME4)
      spark.sql(props.getProperty(CommonConstants.COMMONVAR4) + dbName4)
      spark.sql(props.getProperty(CommonConstants.COMMONVAR3))
      spark.sql(props.getProperty(CommonConstants.RAWTBLNAME))
      spark.sql(props.getProperty(CommonConstants.RAWTBLNAME_P1))
      spark.sql(props.getProperty(CommonConstants.RAWTBLNAME_P2))
      //spark.sql(props.getProperty(CommonConstants.RAWTBLNAMEENC))
      
      spark.sql(tableLocation(0) + "=" + "'" + envProps.getProperty(EnvConstants.RAWBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocation(1) + "'")
      val tableLocationP1 = props.getProperty(CommonConstants.RAWTBLLOCATION_P1).split("=")
      spark.sql(tableLocationP1(0) + "=" + "'" + envProps.getProperty(EnvConstants.RAWBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocationP1(1) + "'")
      val tableLocationP2 = props.getProperty(CommonConstants.RAWTBLLOCATION_P2).split("=")
      spark.sql(tableLocationP2(0) + "=" + "'" + envProps.getProperty(EnvConstants.RAWBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocationP2(1) + "'")
      //encryption path formation
      //val enctableLocation = props.getProperty(CommonConstants.RAWTBLLOCATION_ENC).split("=")
      // println("---------------------------------------11")
      //println("---------------------------------------11")
      //println("---------------------------------------11" + enctableLocation(0) + "=" + "'" + envProps.getProperty(EnvConstants.ENCTEMPPATH) + CommonConstants.FILE_SEPARATOR + enctableLocation(1) + "'")
      //below variable is used for encryption logic only
      //tempTblLocPath=envProps.getProperty(EnvConstants.RAWBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocation(1)

      // spark.sql(enctableLocation(0) + "=" + "'" + envProps.getProperty(EnvConstants.ENCTEMPPATH) + CommonConstants.FILE_SEPARATOR + enctableLocation(1) + "'")

      spark.sql(props.getProperty(CommonConstants.RAWPARTITIONCOL))
      spark.sql(props.getProperty(CommonConstants.AUDITTBLNAME))
      spark.sql(props.getProperty(CommonConstants.AUDITTBLSCHEMA))
      spark.sql(props.getProperty(CommonConstants.AUDITTABLELOCATION))

    } catch {
      case ex: FileNotFoundException => "Property  file Not Found..."
      case ex: Exception =>
        "Property  file Not Found..." + "Variables are not defined in object class" + ex.printStackTrace()
        exceptionEncountered = true
    }
    //}

    //CREATE DATABASE IF NOT EXISTS
    /*  def createDBIfNotExists() = {*/
    try {
      //logger.info("CREATING RAW DATABASE IF NOT EXISTS ...")
      //spark.sql(props.getProperty(CommonConstants.CREATEDATABASEIFNOTEXISTS_RAW) + CommonConstants.SPACE + envProps.getProperty(EnvConstants.DATABASENAME1))
    } catch {
      case ex: Exception =>
        ex.getMessage
        rawStatus = false
        errorMessage = "RAW DATABASE CREATION FAILED"
        exceptionEncountered = true
    }

    try {
      //logger.info("CREATING AUDIT DATABASE IF NOT EXISTS ...")
      //spark.sql(props.getProperty(CommonConstants.CREATEDATABASEIFNOTEXISTS_RAW) + CommonConstants.SPACE + envProps.getProperty(EnvConstants.DATABASENAME4))
    } catch {
      case ex: Exception =>
        ex.getMessage
        auditStatus = false
        errorMessage = "AUDIT DATABASE CREATION FAILED"
        exceptionEncountered = true
    }
    //}

    //CREATE EXTERNAL TABLE ON RAW LAYER
    /*  def createExtTbl() = {*/
    if (rawStatus == true) {
      try {
        logger.info("CREATE EXTERNAL TABLE ON RAW LAYER")
        if (FullLoadExists == "F") {
          logger.info("----Load type is F .. Tables will be recreated----")
          val tableNameRaw = props.getProperty(CommonConstants.RAWTBLNAME).split("=")
          try{          
          //val queryRaw = "ALTER TABLE " + dbName1 + "." + tableNameRaw(1) + " DROP PARTITION (date_month!='abc')"
          //spark.sql(queryRaw)
        var ingestionDatePartitions1 = spark.sql("select distinct date_month from " + dbName1 + "." + tableNameRaw(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions1)
        {
          var queryAlterRefined = "ALTER TABLE " + dbName1 + "." + tableNameRaw(1) + " DROP PARTITION (date_month = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }  
          spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RAW))
          val queryRawSchema = "SELECT * FROM " + dbName1 + "." + tableNameRaw(1) + " LIMIT 1"
          schema_table = spark.sql(queryRawSchema).columns
          }catch{
            case ex: Exception =>{}
          }
          if (fs.exists(schemaChangeFile)) {
            var inSchemePath = envProps.getProperty(CommonConstants.INCOMINGSCHEMAPATH) + "/" + props.getProperty(CommonConstants.INCOMINGSCHEMAFILE)
            val masterSchemaArr = sparkContext.textFile(inSchemePath).take(1)(0).split("\\|")
            var masterSchemaColsArr = ArrayBuffer[String]()
            for (ele <- 0 to masterSchemaArr.length - 1) {
              masterSchemaColsArr += masterSchemaArr(ele).split(" ")(0) + " STRING"
            }
            var masterSchemaCols = masterSchemaColsArr.mkString(",")
            var incomingSchema = ""
            if (props.getProperty(CommonConstants.PARTIALREFFLAG).toUpperCase() == "Y") {
              incomingSchema = masterSchemaCols + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING,active_flag STRING"
                
            } else {
              incomingSchema = masterSchemaCols + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING"
              logger.info("Into second else>>>>>>>>" + "incomingSchema" + ">>>>>>>>>>>>>" + incomingSchema) 
            }
            val setChangedSchema = "SET hivevar:stgtblschema=" + incomingSchema
            logger.info(">>>>>>>>" + "setChangedSchema" + ">>>>>>>>>>>>>" + setChangedSchema) 
            spark.sql(setChangedSchema)
            spark.sql("DROP TABLE " + dbName1 + "." + tableNameRaw(1))
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW))
          } else {
            spark.sql(props.getProperty(CommonConstants.RAWTBLSCHEMA))
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW))
          }
        } else {
          spark.sql(props.getProperty(CommonConstants.RAWTBLSCHEMA))
          spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW))
        }
      } catch {
        case ex: Exception =>
          {
            spark.sql(props.getProperty(CommonConstants.RAWTBLSCHEMA))
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW))
            print("*************listDatabases*****************")
            spark.catalog.listDatabases.show(false)
            logger.info("RAW TABLE CREATION FAILED", ex.printStackTrace())
            ex.getMessage
            rawStatus = false
            errorMessage = "RAW TABLE CREATION FAILED"
          }
          exceptionEncountered = true
      }
      
      val partitionCols = props.getProperty(CommonConstants.RAWPARTITIONCOL).split("=")(1).split(",")
      var partitionColsWithoutType = ArrayBuffer[String]()
      for(cols <- partitionCols)
      {
        partitionColsWithoutType +=  cols.split(" ")(0).toLowerCase()
      }
     if(p1p2CreateFlag.equalsIgnoreCase("Y"))
     {
      try {
        if (fs.exists(inFileP1) && FullLoadExists == "F") {
          val tableNameRaw_P1 = props.getProperty(CommonConstants.RAWTBLNAME_P1).split("=")
          val queryRawSchema_P1 = "SELECT * FROM " + dbName1 + "." + tableNameRaw_P1(1) + " LIMIT 1"
          //val queryRaw_P1 = "ALTER TABLE " + dbName1 + "." + tableNameRaw_P1(1) + " DROP PARTITION (date_month!='abc')"
          //spark.sql(queryRaw_P1)
        var ingestionDatePartitions2 = spark.sql("select distinct date_month from " + dbName1 + "." + tableNameRaw_P1(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions2)
        {
          var queryAlterRefined = "ALTER TABLE " + dbName1 + "." + tableNameRaw_P1(1) + " DROP PARTITION (date_month = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }  
          spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RAW_P1))
          schema_table_P1 = spark.sql(queryRawSchema_P1).columns
          if (schema_table_P1.deep != schema_table.deep) {
            var SchemaColsArr_P1 = ArrayBuffer[String]()
            for (ele <- 0 to schema_table.length - 1) {
              if (!partitionColsWithoutType.contains(schema_table(ele).toLowerCase())) {
                SchemaColsArr_P1 += schema_table(ele).toUpperCase() + " STRING"
              }
            }
            var SchemaCols_P1 = SchemaColsArr_P1.mkString(",")
            val setChangedSchema = "SET hivevar:stgtblschema=" + SchemaCols_P1
            spark.sql(setChangedSchema)
            spark.sql("DROP TABLE " + dbName1 + "." + tableNameRaw_P1(1))
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW_P1))
          }
        } else {
          spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW_P1))
        }
      } catch {
        case ex: Exception =>
          {
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW_P1))
            logger.info("P1 RAW TABLE CREATION FAILED", ex.printStackTrace())
            ex.getMessage
          }
          errorMessage = "P1 RAW TABLE CREATION FAILED"
          exceptionEncountered = true
      }
      try {
        if (fs.exists(inFileP2) && FullLoadExists == "F") {
          val tableNameRaw_P2 = props.getProperty(CommonConstants.RAWTBLNAME_P2).split("=")
          val queryRawSchema_P2 = "SELECT * FROM " + dbName1 + "." + tableNameRaw_P2(1) + " LIMIT 1"
          //val queryRaw_P2 = "ALTER TABLE " + dbName1 + "." + tableNameRaw_P2(1) + " DROP PARTITION (date_month!='abc')"
          //spark.sql(queryRaw_P2)
        var ingestionDatePartitions3 = spark.sql("select distinct date_month from " + dbName1 + "." + tableNameRaw_P2(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions3)
        {
          var queryAlterRefined = "ALTER TABLE " + dbName1 + "." + tableNameRaw_P2(1) + " DROP PARTITION (date_month = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }  
          spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RAW_P2))
          schema_table_P2 = spark.sql(queryRawSchema_P2).columns
          if (schema_table_P1.deep != schema_table_P2.deep) {
            var SchemaColsArr_P2 = ArrayBuffer[String]()
            for (ele <- 0 to schema_table_P1.length - 1) {
              if (!partitionColsWithoutType.contains(schema_table_P1(ele).toLowerCase())) {
                SchemaColsArr_P2 += schema_table_P1(ele).toUpperCase() + " STRING"
              }
            }
            var SchemaCols_P2 = SchemaColsArr_P2.mkString(",")
            val setChangedSchema = "SET hivevar:stgtblschema=" + SchemaCols_P2
            spark.sql(setChangedSchema)
            spark.sql("DROP TABLE " + dbName1 + "." + tableNameRaw_P2(1))
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW_P2))
          }
        } else {
          spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW_P2))
        }
      } catch {
        case ex: Exception =>
          {
            spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RAW_P2))
            logger.info("P2 RAW TABLE CREATION FAILED", ex.printStackTrace())
            ex.getMessage
          }
          errorMessage = "P2 TABLE CREATION FAILED"
          exceptionEncountered = true
      }
     }
    }
    //}

    //ALTER TABLE COMMAND FOR NEWLY ADDED COLUMNS
    /*  def alterTbl() = {*/
    if (rawStatus == true) {
      try {
        logger.info("ALTER TABLE ON RAW LAYER")
        if (fs.exists(schemaChangeFile) && FullLoadExists != "F") {
          logger.info("SCHEMA GOT CHANGED!!")
          val inStream = fs.open(schemaChangeFile)
          val buffReader = new BufferedReader(new InputStreamReader(inStream))
          val new_columns = buffReader.readLine()
          val alter_query = props.getProperty(CommonConstants.ALTERTABLE_RAW) + "(" + new_columns + ")"
          val alter_query_p1 = props.getProperty(CommonConstants.ALTERTABLE_RAW_P1) + "(" + new_columns + ")"
          val alter_query_p2 = props.getProperty(CommonConstants.ALTERTABLE_RAW_P2) + "(" + new_columns + ")"
          spark.sql(alter_query)

          /*var inSchemePath = envProps.getProperty(CommonConstants.INCOMINGSCHEMAPATH) + "/" + props.getProperty(CommonConstants.INCOMINGSCHEMAFILE)
          val masterSchemaArr = sparkContext.textFile(inSchemePath).take(1)(0).split("\\|")
          var masterSchemaColsArr = ArrayBuffer[String]()
          for (ele <- 0 to masterSchemaArr.length - 1) {
            masterSchemaColsArr += masterSchemaArr(ele).split(" ")(0)+" STRING"
          }
          var masterSchemaCols = masterSchemaColsArr.mkString(",")
          var incomingSchema = ""
          if (props.getProperty(CommonConstants.PARTIALREFFLAG).toUpperCase() == "Y") {
            incomingSchema = masterSchemaCols + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING,active_flag STRING"

          } else {
            incomingSchema = masterSchemaCols + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING"
          }          
          val setChangedSchema = "SET hivevar:stgtblschemaChange=" + incomingSchema
          spark.sql(setChangedSchema)

          val tableCreateQuerySchemaChange = "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:db}.${hivevar:stgtable} (${hivevar:stgtblschemaChange}) PARTITIONED BY (${hivevar:stgtblpartitioncols}) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '|' STORED AS PARQUET LOCATION ${hivevar:stgtbllocation}"
          val tableCreateQuerySchemaChange_p1 = "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:db}.${hivevar:stgtablep1} (${hivevar:stgtblschemaChange}) PARTITIONED BY (${hivevar:stgtblpartitioncols}) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '|' STORED AS PARQUET LOCATION ${hivevar:stgtbllocationp1}"
          val tableCreateQuerySchemaChange_p2 = "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:db}.${hivevar:stgtablep2} (${hivevar:stgtblschemaChange}) PARTITIONED BY (${hivevar:stgtblpartitioncols}) ROW FORMAT DELIMITED FIELDS TERMINATED BY  '|' STORED AS PARQUET LOCATION ${hivevar:stgtbllocationp2}"
          val droptableSchemaChange = "DROP TABLE ${hivevar:db}.${hivevar:stgtable}"
          val droptableSchemaChange_p1 = "DROP TABLE ${hivevar:db}.${hivevar:stgtablep1}"
          val droptableSchemaChange_p2 = "DROP TABLE ${hivevar:db}.${hivevar:stgtablep2}"
          spark.sql(droptableSchemaChange)
          System.out.println(tableCreateQuerySchemaChange)
          spark.sql(tableCreateQuerySchemaChange)*/

          if (fs.exists(inFileP1)) {
            //spark.sql(alter_query_p1)
          }
          if (fs.exists(inFileP2)) {
            //spark.sql(alter_query_p2)
          }
        } else
          logger.info("NO CHANGE IN SCHEMA!!")

      } catch {
        case ex: Exception =>
          {
            ex.getMessage
            //rawStatus = false
            errorMessage = "ALTER TABLE FAILED"
          }
          logger.error(errorMessage)
          exceptionEncountered = true
      }
    }
    // }

    //UPDATE HIVE METASTORE FOR NEWLY ADDED PARTITIONS
    /*  def updateMetaData() = {*/
    logger.info("UPDATE HIVE METASTORE FOR NEWLY ADDED PARTITIONS>>>>>rawStatus=" + rawStatus)
    if (rawStatus == true || rawStatus == false) {
      try {
        logger.info("UPDATE HIVE METASTORE FOR NEWLY ADDED PARTITIONS")
        spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RAW))
        if(p1p2CreateFlag.equalsIgnoreCase("Y")){
        if (fs.exists(inFileP1)) {
             spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RAW_P1))       
        }

        if (fs.exists(inFileP2)) {
          spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RAW_P2))
        }
        }
      } catch {
        case ex: Exception =>
          "Partitions not found...." + {
            logger.error("MSCK REPAIR FAILED",ex.printStackTrace())
            ex.getMessage
            rawStatus = false
            errorMessage = "MSCK REPAIR FAILED"
          }
          exceptionEncountered = true
      }
    }
    
    //=====================================================

    // CREATE AUDIT TABLE
    /*  def createAuditTbl() = {*/
    if (auditStatus == true) {
      try {
        logger.info("CREATE AUDIT TABLE ON RAW LAYER")
        spark.sql(props.getProperty(CommonConstants.CREATEAUDITTABLE_RAW))
      } catch {
        case ex: Exception =>
          {
            ex.getMessage
            errorMessage = "AUDIT TABLE CREATION FAILED"
          }
          exceptionEncountered = true
      }
    }
    // }

    // WRITING DATA IN AUDIT TABLE
    /*  def writeToAudit() = {*/
    if (auditStatus == true) {
      try {
        val filePath = props.getProperty(CommonConstants.AUDITTABLEFILE);
        var counts = 0.0 // count -  Long
        if (rawStatus == true) {
          counts = spark.sql(props.getProperty(CommonConstants.RECORDCOUNT_RAW)).collect().head.getLong(0)
        }

        val metaDataLoc = envProps.getProperty(CommonConstants.METADATALOCATION)
        val feed_name_loc = props.getProperty(CommonConstants.RAWTBLLOCATION)
        val feed_name = feed_name_loc.split("=")

        val outFile = new Path(filePath)
        var outStream: FSDataOutputStream = null
        if (!fs.exists(outFile)) {
          outStream = fs.create(outFile)

        } else {
          outStream = fs.append(outFile)

        }

        val bufWriter = new BufferedWriter(new OutputStreamWriter(outStream))

        val df = new SimpleDateFormat("HH:mm:ss");
        val calobj = Calendar.getInstance();
        val endtime = df.format(calobj.getTime());

        val fileSize = fs.getContentSummary(outFile).getLength()
        val job_name = "RAW"
        val batch_id = sparkContext.textFile(metaDataLoc + "/loadmetadata.txt").take(1)(0).split("\\,")(0)
        val data_set = envProps.getProperty(EnvConstants.DATASET)
        val vendor = envProps.getProperty(EnvConstants.VENDOR)

        var status = ""

        if (rawStatus == true) {
          status = "Success"

        } else {
          status = "Failure"
        }

        if (fileSize == 0) {
          bufWriter.append(batch_id + "|" + data_set + "|" + vendor + "|" + Market_CD + "|" + feed_name(1) + "|" + job_name + "|" + starttime.toString() + "|" + endtime.toString() + "|" + counts + "|" + status + "|" + errorMessage)
        } else {
          bufWriter.newLine()
          bufWriter.append(batch_id + "|" + data_set + "|" + vendor + "|" + Market_CD + "|" + feed_name(1) + "|" + job_name + "|" + starttime.toString() + "|" + endtime.toString() + "|" + counts + "|" + status + "|" + errorMessage)
        }

        bufWriter.close()
        outStream.close()
      } catch {
        case ex: FileNotFoundException =>
          ("Audit file Not Found. No change in schema") + ex.getMessage
          exceptionEncountered = true
      }
    }
  }
  if (exceptionEncountered) {   
    System.exit(1)
  }
}