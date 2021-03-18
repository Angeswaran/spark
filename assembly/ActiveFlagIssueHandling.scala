package com.humira.dataingestion


import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
//import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
//import akka.io.Tcp.Write

object ActiveFlagIssueHandling {

  def addValuesForRemovedCol(input1: RDD[String], delimeter: String, splitString: String,activeFlagPosition: Int,lastPosition:Int): RDD[String] = {
    var line = ""
    val updatedTextFileRDD = input1.filter { x => !x.isEmpty() }.map { x =>
      line = x
      if (!x.isEmpty()) {
        val rowData = x.split(splitString, -1)
        var rowDataUpdated = ArrayBuffer[String]()
        for (rowColsEle <- 0 to rowData.length - 1) {
          if (rowColsEle == activeFlagPosition) {
            rowDataUpdated += "Y"
            rowDataUpdated += rowData(rowColsEle)
          } else if (rowColsEle == lastPosition) {

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

  def main(arg: Array[String]) {
    val spark = SparkSession.builder().appName("ActiveFlagHandler").getOrCreate()
    val sc = spark.sparkContext
    //var sparkConf = new SparkConf().setAppName("ActiveFlagHandler")
    //var sc = new SparkContext(sparkConf)
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = spark.sqlContext
    sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    import sqlContext.implicits._

    val listFilePath = "/user/svc-cop-loadprod/ActiveFlagIssueFileList.txt"
    val delimeter = "|"
    val splitString = "\\" + delimeter
    val schemaForMapping = "PATIENT_ID|TRANSACTIONID|BRAND|COPAY_HIPAA|COST_CENTER|COUPON_AMOUNT_HIPAA|DATE_FILLED|DATE_WRITTEN|DAW|DAYS_SUPPLY|DOCTOR_ADDRESS_1|DOCTOR_ADDRESS_2|DOCTOR_CITY|DOCTOR_DEA|DOCTOR_DEA2|DOCTOR_FIRST_NAME|DOCTOR_LAST_NAME|DOCTOR_MIDDLE_NAME|DOCTOR_NPI|DOCTOR_STATE|DOCTOR_TITLE_CODE|DOCTOR_ZIP|DRUG_DESCRIPTION|FEE_HIPAA|FULL_NAME|INSURANCE_STATUS_CODE|NEW_REFILL_IND|OTHER_COVERAGE|PA_AUTHORIZATION|PTNT_PAID_AMT_HIPAA|PTNT_PAID_AMT_QUALIFIER|PHARMACY_ID|PRODUCT_NDC|PROGRAM_NAME|PROGRAM_INDICATION|PROGRAM_TYPE|PROVIDER_REIMB_HIPAA|QUANTITY_DISPENSED|REFILLS_AUTHORIZED|STATUS|STRENGTH|TSTAMP|TOTAL_AMT_AUTHORIZED_HIPAA|TRANSACTION_TYPE|CHANNEL|VENDOR|SHA_PROCESSING_MONTH|src_code|logical_src_code|raw_ingestion_time|active_flag|market_cd|date_month|expiry_date|DQM_Status".split(splitString)
    val textSchemaStruct = StructType(schemaForMapping.map { fieldName => StructField(fieldName, StringType, true) })
    val activeFlagPosition = 50
    val lastPosition = 54

    val listFileRDD = sc.textFile(listFilePath)
    val listFileArray = listFileRDD.collect()
    for (col <- listFileArray.par) {
      val index1 = listFileArray.indexOf(col)
      val filePath = listFileArray(index1)
      println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"+filePath)

      val df = sqlContext.read.parquet(filePath)
      df.show()
      var convertdftordd = df.rdd
      convertdftordd.take(2).foreach(println)
      val rawFileRdd = convertdftordd.map { x => x.mkString(delimeter) }
      var rawfile = convertdftordd.map { x => x.mkString(delimeter) }
      rawfile.take(2).foreach(println)
      val textFileRDDUpdated = addValuesForRemovedCol(rawfile, delimeter,splitString, activeFlagPosition,lastPosition).map(_.split(splitString, -1)).map(x => x.toSeq).map { x => Row.fromSeq(x) }
      textFileRDDUpdated.take(2).foreach(println)

      val outputPath = filePath + "_temp"

      var textDF = sqlContext.createDataFrame(textFileRDDUpdated, textSchemaStruct)
      textDF.write.parquet(outputPath)

    }
  }

}