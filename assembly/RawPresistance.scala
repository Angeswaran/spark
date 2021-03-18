package com.humira.dataingestion

import java.util.Properties

//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession

import com.humira.utilities.CustomLogger


/**
 * *****************************************************************************************
 *  @author By CTS BIGDATA TEAM
 *  @version 2.10.5
 *  @Created on : 12-08-2016
 *  @Revised on : 20-08-2016
 *
 * @Description:
 *  Class for executing the Persistence Layer 1.
 * 	Here we have perform 3 main steps:
 * 	STEP 1 : Creating Staging Table on top of S3
 * 	STEP 2 : Adding Data Lineage Columns
 * 	STEP 3 : Persisting Layer 1 on S3
 * *****************************************************************************************
 */
class RawPersistence() {

  var props: Properties = null
  //  var envProps: Properties = null
  var propsDQ: Properties = null
  val spark : SparkSession = null
  //var sparkConf: SparkConf = null
  //var sparkContext : SparkSession = null
  //var hiveContext: HiveContext = null
  var envProps : Properties = null

  //creating the logger instance
  val logger = new CustomLogger().getLogger(this.getClass.toString())
  /*
   * Read properties file for layer
   */
  def readProperties(propertyFilePath: String) {
    props = new ReadPropeties().readproperty(propertyFilePath)
  }

  def readDQProperties(propertyFilePath: String) {
    propsDQ = new ReadPropeties().readproperty(propertyFilePath)
  }
  def readEnvProperties(propertyFilePath :String){
    envProps = new ReadPropeties().readproperty(propertyFilePath)
  }
  /*
   * Set Spark and Hive Configurations
   */
  def setConfigurations() {
    //sparkConf = new SparkConf().setAppName("")
    //sparkContext = new SparkContext(sparkConf)
    //hiveContext = new HiveContext(sparkContext)
    val spark = SparkSession.builder().appName("").getOrCreate()
    val sparkContext = spark.sparkContext
      
  }

  /*
   * Stop Spark Context
   */
  def stopSparkContext() {
    spark.sparkContext.stop()

  }
   def setCommonHiveVars() = {
    //hiveContext.sql(envProps.getProperty(C360EnvConstants.HIVEVARFORDB)+"="+envProps.getProperty(C360EnvConstants.HIVEDBNAMELAYER1))
    //hiveContext.sql(envProps.getProperty(C360EnvConstants.HIVEVARFORDBLOCATION)+"/"+envProps.getProperty(C360EnvConstants.HIVEDBNAMELAYER1+".db"))
    //hiveContext.sql(AbbvieConstants.SETHIVEVAR+envProps.getProperty(AbbvieEnvConstants.DATABASENAME1));
   
   // hiveContext.sql(C360CommonConstants.SETHIVEDBPATH+envProps.getProperty(C360EnvConstants.HIVEVARFORDB)+"/"+envProps.getProperty(C360EnvConstants.HIVEDBNAMELAYER1)+".db");
  }

  /*
   * Sub Driver main method for layer 1
   */
  def main1(arg: Array[String]) {

    logger.info(arg(0) + " " + arg(1) + " " + arg(2))
    val propertyFilePath = arg(0)
    readProperties(propertyFilePath)
    val EnvPropertyFilePath = arg(2)
    readEnvProperties(EnvPropertyFilePath)
    
    setConfigurations()
    setCommonHiveVars()
    
    
/*    logger.info("STEP 1 : Creating Staging Table on top of HDFS ...")
    val StgObj = new RawLayer(props, hiveContext, sparkContext,envProps)
    StgObj.setHiveVars()
    StgObj.createDBIfNotExists()
    StgObj.createExtTbl()
    StgObj.alterTbl()
    StgObj.updateMetaData()
    StgObj.createAuditTbl()
    StgObj.writeToAudit()*/
    
    
    logger.info("Stopping Spark Context ...")
    stopSparkContext()
  }

}