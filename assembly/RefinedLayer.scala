package com.humira.dataingestion

import com.humira.utilities.CustomLogger
import com.humira.constants.{ CommonConstants, EnvConstants, DataQualityConstants }

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import scala.io.Source
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileNotFoundException
import java.io.IOException
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import org.apache.spark.sql.SparkSession

import scala.util.control.Exception._

class RefinedLayer() {
  def readProperties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)

  }
  def readENVProperties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)

  }
  val spark = SparkSession.builder().appName("Refined").config("hive.exec.dynamic.partition", "true")
              .config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
  //var sparkConf = new SparkConf().setAppName("Refined")
  var sparkContext = spark.sparkContext
  //var sparkContext = new SparkContext(sparkConf)
  //import org.apache.spark.sql.hive.spark
  //val spark = new spark(sparkContext)
  //spark.setConf("hive.exec.dynamic.partition", "true")
  //spark.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
  val conf = sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)

  var exceptionEncountered: Boolean = false

  val logger = new CustomLogger().getLogger(this.getClass().toString())
  def main(args: Array[String]) {
    var props: Properties = readProperties(args(0))
    var envProps: Properties = readENVProperties(args(2))
    val df = new SimpleDateFormat("HH:mm:ss");
    val calobj = Calendar.getInstance();
    val starttime = df.format(calobj.getTime());
    val partitioncol1 = "ingestion_dt_tm"
    val listFilePath = envProps.getProperty(CommonConstants.FILELISTCURRENTABSOLUTEPATH) + "/" + props.getProperty(CommonConstants.FILELISTNAME)
    val listFileRDD = sparkContext.textFile(listFilePath)
    val listFileArray = listFileRDD.collect() //collect RDD into an Array    
    var splitString = "\\" + props.getProperty(CommonConstants.RAWTBLDELIMITER) 
    val isStringQualifierDoubleQuotes = props.getProperty(CommonConstants.ISSTRINGQUALIFIERDOUBLEQUOTES)
   
    if(!(isStringQualifierDoubleQuotes.equalsIgnoreCase("Y"))){
     splitString = splitString + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
    }
    
    var FullLoadExists = ""
    val p1p2CreateFlag = props.getProperty(CommonConstants.P1P2CREATEFLAG)
    
    FullLoadExists = listFileArray(0).split(splitString)(4)
    val Market_CD = envProps.getProperty(CommonConstants.MARKETCD)
    val tableLocation = props.getProperty(CommonConstants.RFDTBLLOCATION).split("=")
    val tableLocationP1 = props.getProperty(CommonConstants.RFDTBLLOCATION_P1).split("=")
    val tableLocationP2 = props.getProperty(CommonConstants.RFDTBLLOCATION_P2).split("=")
    val tablePathComplete = envProps.getProperty(EnvConstants.REFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocation(1)
    val preTablePathComplete = envProps.getProperty(EnvConstants.PREREFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocation(1)
    val tablePathCompleteP1 = envProps.getProperty(EnvConstants.REFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocationP1(1)
    val tablePathCompleteP2 = envProps.getProperty(EnvConstants.REFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + tableLocationP2(1)
    val metaDataLoc = envProps.getProperty(CommonConstants.METADATALOCATION)
    val incomingSchemaFileString = props.getProperty(CommonConstants.RFDNEWSCHEMASTRING)
    val feedname = props.getProperty(CommonConstants.MASTERSCHEMAFILENAME).replace(".txt", "")
    val batch_id = sparkContext.textFile(metaDataLoc + "/loadmetadata.txt").take(1)(0).split("\\,")(0)
    var rfdStatus: Boolean = true
    var auditStatus: Boolean = true
    var errorMessage = ""
    val infilePath = envProps.getProperty(EnvConstants.STRINGCHANGEDSCHEMAPATH) + "/" + feedname + "/" + incomingSchemaFileString
    val inFile = new Path(infilePath)
    val incomingSchemaFileActual = props.getProperty(CommonConstants.RFDNEWSCHEMAACTUAL)
    val infilePath_view = envProps.getProperty(EnvConstants.ACTUALCHANGEDSCHEMAPATH) + "/" + feedname + "/" + incomingSchemaFileActual
    val inFile_view = new Path(infilePath_view)
    

    val dbNameRefined = envProps.getProperty(EnvConstants.DATABASENAME2)
    //val tablNameRedinedGR = props.getProperty(CommonConstants.RFDTBLNAME_VIEW).split("=")
    spark.sql(props.getProperty(CommonConstants.COMMONVAR2) + dbNameRefined)
   /* try {
      var queryRedinedGR = "ALTER TABLE " + dbNameRefined + "." + tablNameRedinedGR(1) + " DROP PARTITION (ingestion_dt_tm!='abc')"
      spark.sql(queryRedinedGR)
    } catch {
      case e: Exception =>
        {
          logger.error(e.getMessage)
        }
    }*/

    val tablNameRedined = props.getProperty(CommonConstants.RFDTBLNAME).split("=")
    val tablNameRedined_P2 = props.getProperty(CommonConstants.RFDTBLNAME_P2).split("=")
    val tableNameRefined_P1 = props.getProperty(CommonConstants.RFDTBLNAME_P1).split("=")
    try {
      val dbName1 = envProps.getProperty(EnvConstants.DATABASENAME2)
      spark.sql(props.getProperty(CommonConstants.COMMONVAR2) + dbName1)
      spark.sql(props.getProperty(CommonConstants.RFDTBLNAME))
     // spark.sql(props.getProperty(CommonConstants.RFDTBLNAME_VIEW))
      spark.sql(props.getProperty(CommonConstants.RFDTBLLOCATION))
      //  spark.sql(props.getProperty(CommonConstants.RFDTBLDELIMITER))
     // spark.sql(props.getProperty(CommonConstants.RFDTBLSCHEMA_VIEW))
      spark.sql(props.getProperty(CommonConstants.RFDTBLSCHEMA))
      spark.sql(props.getProperty(CommonConstants.RFDPARTITIONCOL))
      spark.sql(props.getProperty(CommonConstants.RFDTBLNAME_P1))
      spark.sql(props.getProperty(CommonConstants.RFDTBLNAME_P2))

      spark.sql(tableLocation(0) + "=" + "'" + tablePathComplete + "'")
      spark.sql(tableLocationP1(0) + "=" + "'" + tablePathCompleteP1 + "'")
      spark.sql(tableLocationP2(0) + "=" + "'" + tablePathCompleteP2 + "'")
    } catch {
      case ex: FileNotFoundException => { println("Property  file Not Found...") }
      case ex: Exception =>
        { println("Variables are not defined in object class") }
        exceptionEncountered = true
    }

    if (FullLoadExists == "F") {
      logger.info("----Load type is F ..")
      var schema_table = Array[String]()
      var schema_table_P1 = Array[String]()
      var schema_table_P2 = Array[String]()
      try {
        val queryRawSchema = "SELECT * FROM " + dbNameRefined + "." + tablNameRedined(1) + " LIMIT 1"
        schema_table = spark.sql(queryRawSchema).columns
        if(p1p2CreateFlag.equalsIgnoreCase("Y")){
        val queryRawSchema_P2 = "SELECT * FROM " + dbNameRefined + "." + tablNameRedined_P2(1) + " LIMIT 1"
        try{
        schema_table_P2 = spark.sql(queryRawSchema_P2).columns
        }catch{
          case e: Exception =>
          {
            logger.info("Refined P2 Table does not exist", e)
          }
        }
        val queryRefined_Schema_P1 = "SELECT * FROM " + dbNameRefined + "." + tableNameRefined_P1(1) + " LIMIT 1"
        try{
        schema_table_P1 = spark.sql(queryRefined_Schema_P1).columns
        }catch{
          case e: Exception =>
          {
            logger.info("Refined P1 Table does not exist", e)
          }
        }
        }
        if (fs.exists(inFile) && fs.exists(inFile_view)) {
          var inSchemePath = envProps.getProperty(CommonConstants.INCOMINGSCHEMAPATH) + "/" + props.getProperty(CommonConstants.INCOMINGSCHEMAFILE)
          val masterSchemaArr = sparkContext.textFile(inSchemePath).take(1)(0).split("\\|")
          var masterSchemaColsArr = ArrayBuffer[String]()
          var masterSchemaColsArr_Original = ArrayBuffer[String]()
          for (ele <- 0 to masterSchemaArr.length - 1) {
            masterSchemaColsArr += masterSchemaArr(ele).split(" ")(0) + " STRING"
            masterSchemaColsArr_Original += masterSchemaArr(ele).split(" ")(0) + " " + masterSchemaArr(ele).split(" ")(1)
          }
          var masterSchemaCols = masterSchemaColsArr.mkString(",")
          var masterSchemaCols_Original = masterSchemaColsArr_Original.mkString(",")
          var incomingSchema = ""
          var incomingSchema_Original = ""
          if (props.getProperty(CommonConstants.PARTIALREFFLAG).toUpperCase() == "Y") {
            incomingSchema = masterSchemaCols + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING,active_flag STRING,date_month STRING,expiry_date STRING,DQM_Status STRING"
            incomingSchema_Original = masterSchemaCols_Original + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING,active_flag STRING,date_month STRING,expiry_date STRING,DQM_Status STRING"

          } else {
            incomingSchema = masterSchemaCols + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING,date_month STRING,expiry_date STRING,DQM_Status STRING"
            incomingSchema_Original = masterSchemaCols_Original + ",src_code STRING,logical_src_code STRING,raw_ingestion_time STRING,date_month STRING,expiry_date STRING,DQM_Status STRING"
          }
          val setChangedSchema = "SET hivevar:rfdtblschemastring=" + incomingSchema
          spark.sql(setChangedSchema)
          val setChangedSchema_Original = "SET hivevar:ptblschema=" + incomingSchema_Original
          spark.sql(setChangedSchema_Original)

          spark.sql("DROP TABLE " + dbNameRefined + "." + tablNameRedined(1))
          spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD))
          //spark.sql("DROP TABLE " + dbNameRefined + "." + tablNameRedinedGR(1))
          //spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD_VIEW))
          //var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined(1) + " DROP PARTITION (ingestion_dt_tm!='abc')"
          //spark.sql(queryAlterRefined)
        var ingestionDatePartitions2 = spark.sql("select distinct ingestion_dt_tm from " + dbNameRefined + "." + tablNameRedined(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions2)
        {
          var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined(1) + " DROP PARTITION (ingestion_dt_tm = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }
        } else {
        var ingestionDatePartitions3 = spark.sql("select distinct ingestion_dt_tm from " + dbNameRefined + "." + tablNameRedined(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions3)
        {
          var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined(1) + " DROP PARTITION (ingestion_dt_tm = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }
          //var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined(1) + " DROP PARTITION (ingestion_dt_tm!='abc')"
          //spark.sql(queryAlterRefined)
        }
      } catch {
        case e: Exception =>
          {
            logger.info("Refined Table Drop Create Failed", e.printStackTrace())
          }

      }
      //Refined - Change Pointers
      if(p1p2CreateFlag.equalsIgnoreCase("Y"))
      {
      try {
        fs.delete(new Path(envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + "_P2"), true)
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not delete " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + feedName + "_P2 " + e.getMessage
            logger.error("Could not delete " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + "_P2 " + e.getMessage)
            e.printStackTrace()
          }

      }
      val partitionCols = props.getProperty(CommonConstants.RFDPARTITIONCOL).split("=")(1).split(",")
      var partitionColsWithoutType = ArrayBuffer[String]()
      for (cols <- partitionCols) {
        partitionColsWithoutType += cols.split(" ")(0).toLowerCase()
      }
      //Step 2: re-name _P1 (prior 1) to _P2
      try {
        fs.rename(new Path(envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + "_P1"), new Path(envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + "_P2"))
        //val queryRaw_P2 = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined_P2(1) + " DROP PARTITION (ingestion_dt_tm!='abc')"
        //val queryRaw_P2 = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined_P2(1) + " drop if exists partition (ingestion_dt_tm <> 'abc')"
        //spark.sql(queryRaw_P2)
        var ingestionDatePartitions4 = spark.sql("select distinct ingestion_dt_tm from " + dbNameRefined + "." + tablNameRedined_P2(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions4)
        {
          var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined_P2(1) + " DROP PARTITION (ingestion_dt_tm = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }
        if (schema_table_P1.deep != schema_table_P2.deep) {
          var SchemaColsArr_P2 = ArrayBuffer[String]()
          for (ele <- 0 to schema_table_P1.length - 1) {
            if (!partitionColsWithoutType.contains(schema_table_P1(ele).toLowerCase())) {
              SchemaColsArr_P2 += schema_table_P1(ele).toUpperCase() + " STRING"
            }
          }
          var SchemaCols_P2 = SchemaColsArr_P2.mkString(",")
          val setChangedSchema = "SET hivevar:rfdtblschemastring=" + SchemaCols_P2
          spark.sql(setChangedSchema)
          spark.sql("DROP TABLE " + dbNameRefined + "." + tablNameRedined_P2(1))
          spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD_P2))
        }
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not rename " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + feedName + "_P1 " + e.getMessage
            logger.error("Could not rename " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + "_P1 " + e.getMessage)
            e.printStackTrace()
          }

      }
      //Step 3: re-name original dir to _P1

      try {
        fs.rename(new Path(envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName)), new Path(envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + "_P1"))
        //val queryRefined_P1 = "ALTER TABLE " + dbNameRefined + "." + tableNameRefined_P1(1) + " DROP PARTITION (ingestion_dt_tm!='abc')"
        //val queryRefined_P1 = "ALTER TABLE " + dbNameRefined + "." + tableNameRefined_P1(1) + " drop if exists partition (ingestion_dt_tm <> 'abc')"
        //spark.sql(queryRefined_P1)
        var ingestionDatePartitions5 = spark.sql("select distinct ingestion_dt_tm from " + dbNameRefined + "." + tableNameRefined_P1(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions5)
        {
          var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tableNameRefined_P1(1) + " DROP PARTITION (ingestion_dt_tm = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }
        if (schema_table.deep != schema_table_P1.deep) {
          var SchemaColsArr_P1 = ArrayBuffer[String]()
          for (ele <- 0 to schema_table.length - 1) {
            if (!partitionColsWithoutType.contains(schema_table(ele).toLowerCase())) {
              SchemaColsArr_P1 += schema_table(ele).toUpperCase() + " STRING"
            }
          }
          var SchemaCols_P1 = SchemaColsArr_P1.mkString(",")
          val setChangedSchema = "SET hivevar:rfdtblschemastring=" + SchemaCols_P1
          spark.sql(setChangedSchema)
          spark.sql("DROP TABLE " + dbNameRefined + "." + tableNameRefined_P1(1))
          spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD_P1))
        }
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not rename " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + feedName + e.getMessage
            logger.error("Could not rename " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + e.getMessage)
            e.printStackTrace()
          }

      }
      
      }
      //Step 4: remove original dir
      try {
        fs.delete(new Path(envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName)), true)
      } catch {
        case e: Exception =>
          {
            //landToRawFailReason = "Could not delete " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + feedName + e.getMessage
            logger.error("Could not delete " + envProps.getProperty(EnvConstants.REFINEDBASEPATH) + "/" + props.getProperty(CommonConstants.feedName) + e.getMessage)
            e.printStackTrace()
          }

      }
    }

   /* Code start to handle Approval Logic*/
    try {
      val refinedfeedmetadata = sparkContext.textFile(envProps.getProperty(CommonConstants.METADATALOCATION) + "/" + props.getProperty(CommonConstants.feedName) + "_refinedmetadata.txt").take(1).mkString(",")
      val refinedfeedMetadataArr = refinedfeedmetadata.split("\\|")
      val loadType = refinedfeedMetadataArr(0)
      val ingestionDT = refinedfeedMetadataArr(1)
      val preTablePathCompleteWithPartitions = preTablePathComplete + CommonConstants.FILE_SEPARATOR + ingestionDT
      val filesToMove = preTablePathCompleteWithPartitions + "/*"
      val filesCleanUp = preTablePathCompleteWithPartitions
      val tablePathCompleteWithPartitions = tablePathComplete + CommonConstants.FILE_SEPARATOR + ingestionDT

      if (loadType == "P") {
        val refinedPartialParitions = refinedfeedMetadataArr(2)
        val refinedPartialParitionsArr = refinedPartialParitions.split(",")
        for (refinedPartialParition <- refinedPartialParitionsArr) {
          val preTablePathCompleteWithPartialPartitions = preTablePathComplete + "/" + refinedPartialParition
          val filesToMovePartial = preTablePathCompleteWithPartialPartitions + "/*"
          val filesCleanUpPartial = preTablePathCompleteWithPartialPartitions
          val tablePathCompleteWithPartialPartitions = tablePathComplete +"/"+ refinedPartialParition
          val refAffectedPartPathCleanUp = tablePathComplete + "/"+refinedPartialParition + "/*"
          println("tablePathCompleteWithPartialPartitions is"+tablePathCompleteWithPartialPartitions +"***and refAffectedPartPathCleanUp is"+refAffectedPartPathCleanUp)

          Seq("hadoop", "fs", "-rm", refAffectedPartPathCleanUp).!
          Seq("hadoop", "fs", "-mv", filesToMovePartial, tablePathCompleteWithPartialPartitions).!
          Seq("hadoop", "fs", "-rm", "-r", filesCleanUpPartial).!
        }
      }
      Seq("hadoop", "fs", "-mkdir", tablePathComplete).!
      Seq("hadoop", "fs", "-mkdir", tablePathCompleteWithPartitions).!
      Seq("hadoop", "fs", "-mv", filesToMove, tablePathCompleteWithPartitions).!
      Seq("hadoop", "fs", "-rm", "-r", filesCleanUp).!
    } catch {
      case ex: Exception =>
        ex.getMessage
        rfdStatus = false
        errorMessage = "DATA COPY TO REFINED LAYER FAILED"
        exceptionEncountered = true
    }
    /* Code end to handle Approval Logic*/

    val p1 = props.getProperty(CommonConstants.RFDTBLLOCATION_P1).split("=")
    val p2 = props.getProperty(CommonConstants.RFDTBLLOCATION_P2).split("=")
    val infilePathP1 = envProps.getProperty(EnvConstants.REFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + p1(1)
    val infilePathP2 = envProps.getProperty(EnvConstants.REFINEDBASEPATH) + CommonConstants.FILE_SEPARATOR + p2(1)

    val inFileP1 = new Path(infilePathP1)
    val inFileP2 = new Path(infilePathP2)

    //CREATE DATABASE IF NOT EXISTS
    try {
      //logger.info("CREATING DATABASE IF NOT EXISTS ...")
      //spark.sql(props.getProperty(CommonConstants.CREATEDATABASEIFNOTEXISTS_RFD) + CommonConstants.SPACE + envProps.getProperty(EnvConstants.DATABASENAME2))
    } catch {
      case ex: Exception =>
        ex.getMessage
        rfdStatus = false
        errorMessage = "DATABASE CREATION FAILED"
        exceptionEncountered = true
    }
    /*
     * CREATE EXTERNAL TABLE ON REFINED LAYER
     */

    try {
      logger.info("CREATE EXTERNAL TABLE ON REFINED LAYER")
      spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD))
      //spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD_VIEW))
    } catch {
      case ex: Exception =>
        { println("Failed to create Refined tables") }
        rfdStatus = false
        errorMessage = "REFINED TABLE CREATION FAILED"
        exceptionEncountered = true
    }
    if(p1p2CreateFlag.equalsIgnoreCase("Y")){
    try {
      if (fs.exists(inFileP1)) {
        spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD_P1))
      }
    } catch {
      case ex: Exception =>
        { ex.getMessage }
        errorMessage = "P1 TABLE CREATION FAILED"
        logger.info(ex.getMessage)
        exceptionEncountered = true
    }
    try {
      if (fs.exists(inFileP2)) {
        spark.sql(props.getProperty(CommonConstants.CREATEEXTERNALTABLE_RFD_P2))
      }
    } catch {
      case ex: Exception =>
        { ex.getMessage }
        errorMessage = "P2 TABLE CREATION FAILED"
        logger.info(ex.getMessage)
        exceptionEncountered = true
    }
    }

    if (rfdStatus == true) {
      //ALTER TABLE COMMAND FOR NEWLY ADDED COLUMNS
      try {
        logger.info("ALTER TABLE ON REFINED LAYER")
        if (fs.exists(inFile) && fs.exists(inFile_view) && FullLoadExists != "F") {
          logger.info("SCHEMA GOT CHANGED!!")
          val inStream = fs.open(inFile)
          val buffReader = new BufferedReader(new InputStreamReader(inStream))
          val new_columns = buffReader.readLine()
          val inStream_view = fs.open(inFile_view)
          val buffReader_view = new BufferedReader(new InputStreamReader(inStream_view))
          val new_columns_view = buffReader_view.readLine()
          val alter_query = props.getProperty(CommonConstants.ALTERTABLE_RFD) + "(" + new_columns + ")"
          val alter_query_view = props.getProperty(CommonConstants.ALTERTABLE_RFD_VIEW) + "(" + new_columns_view + ")"
          val alter_query_p1 = props.getProperty(CommonConstants.ALTERTABLE_RFD_P1) + "(" + new_columns + ")"
          val alter_query_p2 = props.getProperty(CommonConstants.ALTERTABLE_RFD_P2) + "(" + new_columns + ")"
          spark.sql(alter_query)
          spark.sql(alter_query_view)
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
            //rfdStatus = false
            logger.info("-------------------------------->" + ex.getMessage)
            errorMessage = "ALTER TABLE FAILED"
          }
          exceptionEncountered = true
      } finally {
        if (fs.exists(inFile) && fs.exists(inFile_view)) {
          fs.delete(inFile, true)
          fs.delete(inFile_view, true)
        }

      }
    }
    //UPDATE HIVE METASTORE FOR NEWLY ADDED PARTITIONS
    if (rfdStatus == true || rfdStatus == false) {
      try {
        logger.info("UPDATE HIVE METASTORE FOR NEWLY ADDED PARTITIONS")
        //var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined(1) + " DROP PARTITION (ingestion_dt_tm!='abc')"
        var ingestionDatePartitions1 = spark.sql("select distinct ingestion_dt_tm from " + dbNameRefined + "." + tablNameRedined(1)).collect().map(e => e.toString().replace("[","").replace("]",""))
        for(inDate <- ingestionDatePartitions1)
        {
          var queryAlterRefined = "ALTER TABLE " + dbNameRefined + "." + tablNameRedined(1) + " DROP PARTITION (ingestion_dt_tm = '" + inDate + "' )"
          spark.sql(queryAlterRefined)
        }
        //spark.sql(queryAlterRefined)
        spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_REFINED))
        //spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_REFINED_VIEW))
if(p1p2CreateFlag.equalsIgnoreCase("Y")){
        if (fs.exists(inFileP1)) {
          if (FullLoadExists == "F") {
            spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RFD_P1))
          }
        }
        if (fs.exists(inFileP2)) {
          if (FullLoadExists == "F") {
            spark.sql(props.getProperty(CommonConstants.UPDATETBLPARTITIONS_RFD_P2))
          }
        }
      } 
        }catch {
        case ex: Exception =>
          "Partitions not found...." + {
            ex.getMessage
            rfdStatus = false
            errorMessage = "MSCK REPAIR FAILED"
          }
          exceptionEncountered = true
      }
      
    }
//****************************************************************************************
/*    
    // Inserting good records into another table
    if (rfdStatus == true) {
      try {
        logger.info("Inserting good records into another table")
        spark.sql(props.getProperty(CommonConstants.INSERTINTOTABLE_RFD_VIEW))
      } catch {
        case ex: Exception =>
          {
            println("Table not found." + ex.printStackTrace())
            logger.error(ex.getMessage)
          }
          rfdStatus = false
          exceptionEncountered = true
          errorMessage = "Inserting good records into another table failed"
      }
    }
*/    
    if (rfdStatus == true) {
      try {
        logger.info("Creating the DQM Metric Table...")
        val tableLocation = envProps.getProperty(EnvConstants.DQMTBLLOCATION).split("=")
        spark.sql(envProps.getProperty(EnvConstants.DQMDB))
        spark.sql(envProps.getProperty(EnvConstants.DQMTBL))
        spark.sql(envProps.getProperty(EnvConstants.DQMTBLSCHEMA))
        spark.sql(tableLocation(0) + "='" + envProps.getProperty(EnvConstants.MetricsBASEPATH) + "'")
        spark.sql(envProps.getProperty(EnvConstants.DQMCREATETBL))
      } catch {
        case ex: Exception =>
          { println("DQM Metric Table creation failed.") 
            logger.error(ex.getMessage)}
          rfdStatus = false
          exceptionEncountered = true

          errorMessage = "DQM Metric Table creation failed"

      }
    }

    // WRITING DATA IN AUDIT TABLE
    if (auditStatus == true) {
      try {
        val filePath = props.getProperty(CommonConstants.AUDITFILEPATH)
        // val filePath = "/user/chandbx/audit.txt"

        var counts = 0.0 // count -  Long

        if (rfdStatus == true) {
          counts = spark.sql(props.getProperty(CommonConstants.RECORDCOUNT_RFD)).collect().head.getLong(0)
        }
        val feed_name_prop = props.getProperty(CommonConstants.RFDTBLNAME)

        val feed_name_loc = props.getProperty(CommonConstants.RFDTBLLOCATION)
        //val feed_name = feed_name_loc.split("/")
        //val date_month = feed_name_loc.split("/")

        val outFile = new Path(filePath)

        var outStream: FSDataOutputStream = null
        if (!fs.exists(outFile)) {
          outStream = fs.create(outFile)

        } else {
          outStream = fs.append(outFile)

        }

        var status = ""

        if (rfdStatus == true) {
          status = "Success"

        } else {
          status = "Failure"
        }

        val bufWriter = new BufferedWriter(new OutputStreamWriter(outStream))
        val df = new SimpleDateFormat("HH:mm:ss");
        val calobj = Calendar.getInstance();
        val endtime = df.format(calobj.getTime());

        val job_name = "REF"
        val data_set = envProps.getProperty(EnvConstants.DATASET)
        val vendor = envProps.getProperty(EnvConstants.VENDOR)
        val feed_name = props.getProperty(DataQualityConstants.feedName)

        bufWriter.newLine()
        bufWriter.append(batch_id + "|" + data_set + "|" + vendor + "|" + Market_CD + "|" + feed_name + "|" + job_name + "|" + starttime.toString() + "|" + endtime.toString() + "|" + counts + "|" + status + "|" + errorMessage)

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
