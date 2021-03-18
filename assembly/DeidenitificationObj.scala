package com.humira.dataingestion

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
import org.apache.spark.sql.hive.HiveContext
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
import org.apache.spark.sql.SparkSession

object DeidenitificationObj {
  

	def main(arg: Array[String]) {
    val spark = SparkSession.builder().appName("LandingToRaw2").config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").getOrCreate()
		//var sparkConf = new SparkConf().setAppName("LandingToRaw2")
				//var sparkContext = new SparkContext(sparkConf)
        val sparkContext = spark.sparkContext
				//var hc = new HiveContext(sparkContext)
				//hc.setConf("hive.exec.dynamic.partition", "true")
				//hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
				val conf = sparkContext.hadoopConfiguration
				val fs = org.apache.hadoop.fs.FileSystem.get(conf)
				//val sqlContext = new SQLContext(sparkContext)
        val sqlContext = spark.sqlContext
				sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
				import sqlContext.implicits._

				val feedName = arg(0)
				val patiendRDD = sparkContext.textFile(arg(1))

				val textRowRDD = patiendRDD.map(_.split("\\|", -1)).map(x => x.toSeq).map { x => Row.fromSeq(x) }

		//val schema = "PatientID,PatientName,CityName,ZipCode,Mobile"

		val schema = arg(2)

				val schemaArray = schema.split(",")

				val textSchemaStruct = StructType(schemaArray.map { fieldName => StructField(fieldName, StringType, true) })

				var textDF = sqlContext.createDataFrame(textRowRDD, textSchemaStruct)
				//var szipcode = textDF.col("zipcode")

				val sqlQuery = "select * from master_data.humira_data_security where TABLE_NAME='" + feedName + "' and Active_Flag = 'Y' or Active_Flag = 'y'"

				val deidentifierRDD = spark.sql(sqlQuery).rdd.map { x => x.mkString("|") }
		//val currDataReplacedWithPseudoIdDF: DataFrame = null
		var currentPseudoMapDF: DataFrame = null

				val refrenceTable = deidentifierRDD.collect()
				var zipColumn : String = null
				var columnName : String = null
				var zipCodeDF: DataFrame = null
				refrenceTable.foreach(println)

				for (col <- refrenceTable) {
					val index1 = refrenceTable.indexOf(col)

							// get all the required info from list file
							val value = refrenceTable(index1).split("\\|")
							println("***********************************inside for************************************************")
							columnName = value(1)
							val deidentificationType = value(2)
							var numberOfDigits = 0
							var startOrEnd = "End"
							var maskingCharacter = ""
							if (deidentificationType.equalsIgnoreCase("masking")) {
								numberOfDigits = value(3).toInt
										startOrEnd = value(4)
										maskingCharacter = value(5)
										if(columnName.toLowerCase().contains("zip")){
											zipColumn = columnName
											zipCodeDF = spark.sql("select geoid from master_data.zip_code_population where pop10 <= 20000").coalesce(3)
										}
											else
												zipColumn = null
							}
					println("*****************************************************************************")
					println("columnName:" + columnName)
					println("deidentificationType:" + deidentificationType)
					println("numofdigits:" + numberOfDigits)
					println("startorend:" + startOrEnd)
					println("maskingCharacter:" + maskingCharacter)

					conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, "jceks://hdfs/keystore/epsilonkey.jceks")
					val secretKeyArr = conf.getPassword("epsilonkey.alias") 
					val secretKey = secretKeyArr.mkString("")
					val pseudoKey = secretKey.split("\\|")(0).toLong
					val encryptionKey = secretKey.split("\\|")(1)


					//UDF - to generate pseudo if for original id and then encrypt this pseudo id.
					var pseudoId = ""
					val pseudoIdGeneration = udf[String, String] { (inCol1: String) =>
					pseudoId = inCol1.replace("\"", "")

					if (pseudoId != null && !pseudoId.isEmpty()) {
						//pseudoId = MessageDigest.getInstance("MD5").digest(inCol1.getBytes).map("%02x".format(_)).mkString
						var xorId1 = 0L
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
								} else {

									xorId1 = inCol1.toLong ^ pseudoKey
											val complimentId = (~xorId1).abs
											pseudoId = complimentId.toString()
											/*val rightShiftId= xorId1>>2
              				val xorId2 = rightShiftId^520178
              				val leftShiftId = xorId2<<2
              				val xorId3 = leftShiftId^520178
              				pseudoId = xorId3.toString()*/
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
					var finalID = ""
							val maskedIdGeneration = udf[String, String] { (inCol1: String) =>
							var maskedId = inCol1
							if (maskedId != null && !maskedId.isEmpty()) {
								if (startOrEnd == "End") {
									val unmaskedString = maskedId.substring(0, maskedId.length() - numberOfDigits)
											//repeat the MaskingCharacter as many as numberOfDigits 
											val maskedString = maskingCharacter.toString * numberOfDigits
											maskedId = unmaskedString.concat(maskedString)
								} else {
									val unmaskedString = maskedId.substring(numberOfDigits) // (4 -2 , 4-1) 2,3
											//repeat the MaskingCharacter as many as numberOfDigits 
											val maskedString = maskingCharacter.toString * numberOfDigits
											maskedId = maskedString.concat(unmaskedString)
								}
							}
							maskedId
					}
					if (deidentificationType.equalsIgnoreCase("masking")) {
						//if (textDF.columns.contains("zip"))
						if (zipColumn != null)
						{
							//var zipCodeDF = hc.sql("select geoid from master_data.zip_code_population where pop10 <= 20000")
							//val refzipCode = zipCodeDF.col("geoid")
							var dfjoin = zipCodeDF.as("d2").join(textDF.as("d1"),textDF.col(columnName) === zipCodeDF.col("geoid"), "right_outer").select(zipCodeDF.col("geoid").as("t2zipcode"),$"d1.*")
							val textDF1 = dfjoin.filter(dfjoin("t2zipcode").isNotNull).drop(dfjoin("t2zipcode")).withColumn(columnName, maskedIdGeneration(dfjoin(columnName)))
							val textDF2 = dfjoin.filter(dfjoin("t2zipcode").isNull).drop(dfjoin("t2zipcode"))
							textDF = textDF1.unionAll(textDF2)
							//var zipList = zipCodeDF.select("geoid").map(_.getString(0)).collect.toList 
									//var zipValues = textDF.select(zipColumn).map(_.getString(0)).collect.toList 
									
						}
						else
						{
							textDF = textDF.withColumn(columnName, maskedIdGeneration(textDF(columnName)))
						}
					} else if (deidentificationType.equalsIgnoreCase("Encrypt")) {
						textDF = textDF.withColumn(columnName, EncryptionUDF(textDF(columnName)))
					} else if (deidentificationType.equalsIgnoreCase("Pseudo")) {
						val textDF2 = textDF.withColumn("TEMP_NAME1", textDF(columnName))
								val pseudoTextDF = textDF2.withColumn(columnName, pseudoIdGeneration(textDF(columnName)))
								currentPseudoMapDF = pseudoTextDF.select(pseudoTextDF("TEMP_NAME1"), pseudoTextDF(columnName)).withColumn("TEMP_NAME1", EncryptionUDF(pseudoTextDF("TEMP_NAME1")))
								pseudoTextDF.show()
								currentPseudoMapDF.show()
								/*drop the newly added column*/
								val currDataReplacedWithPseudoIdDF = pseudoTextDF.drop(pseudoTextDF("TEMP_NAME1"))
								textDF = currDataReplacedWithPseudoIdDF
					}
				}
				textDF.show()
				//currentPseudoMapDF.show()
				textDF.write.parquet(arg(3))
				currentPseudoMapDF.write.parquet(arg(4))
	}

}