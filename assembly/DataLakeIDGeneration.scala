package com.humira.dataingestion

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import com.humira.utilities.CustomLogger
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{ col, lit, when, max }
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar
import com.humira.constants.DataIngestionConstants
import org.apache.hadoop.conf.Configuration
import java.util.Properties
import org.apache.hadoop.fs.{ FileSystem, Path }
import com.humira.utilities.DataIngestionUtility
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession

object DataLakeIDGeneration {
  def readPropeties(propertyFilePath: String): Properties = {
    new ReadPropeties().readproperty(propertyFilePath)
  }

  def xRefIfDatalakeIDExistsinHBase(datalakeIdColumnName: String, sparkSession: SparkSession, sqlContext: SQLContext, rawDF: DataFrame,
                                    propertyFile: String): (DataFrame) = {

    import sqlContext.implicits._
    val logger = new CustomLogger().getLogger(this.getClass.toString())
    val readProp = readPropeties(propertyFile)
    val identifierCol: String =readProp.getProperty(DataIngestionConstants.COL_IDENTIFIER_ID)
    val databaseName: String = readProp.getProperty(DataIngestionConstants.DATABASE_NAME)
    val datalakeIDColName: String = readProp.getProperty(DataIngestionConstants.COL_DATA_LAKE_ID)
    val datalakeIDtableName: String = readProp.getProperty(DataIngestionConstants.HTABLE_PATIENT_IDENTIFIER_XREF)
    val xrefTableDB: String = readProp.getProperty(DataIngestionConstants.DEIDENTIFIEDAUDITDATABASE)
    
    val xrefTable = xrefTableDB+"."+datalakeIDtableName

    val dfDataLakeID = sparkSession.sql("select * from " + xrefTable +" where active_flag='Y'")
    val lookUpKeyForPickup = datalakeIDColName
    val lookUpKeyForPickupAlias = "lookup" + datalakeIDColName
    val dfjoin = rawDF.as("d1").join(dfDataLakeID.as("d2"), rawDF(datalakeIdColumnName) === dfDataLakeID(identifierCol), "left_outer").select($"d1.*", dfDataLakeID(lookUpKeyForPickup) as lookUpKeyForPickupAlias)
    
    val dfRenamed = dfjoin.drop(datalakeIdColumnName).withColumnRenamed(lookUpKeyForPickupAlias, datalakeIdColumnName)
    val reOrderCol = rawDF.columns

    val reReplaceDF = dfRenamed.select(reOrderCol.head, reOrderCol.tail: _*)
    reReplaceDF
  }
}