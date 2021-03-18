package com.humira.utilities

import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date
import util.Random.nextInt
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.Properties
import java.io.FileInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.sql.SQLContext
import sys.process._
import org.apache.hadoop.fs.FSDataOutputStream
import scala.util.Try

import com.humira.constants.DataIngestionConstants

/**
 * @author CTS Commercial Datalake Team
 *
 */
object DataIngestionUtility {

	def formattedTimeStamp() : String = {
			val today = Calendar.getInstance().getTime
					val formatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
					val curTime = formatted.format(today)
					curTime
	}

	def formattedKeyTimeStamp() : String = {
			val today = Calendar.getInstance().getTime
					val formatted = new SimpleDateFormat("yyyyMMddHHmmss")
					val curTime = formatted.format(today)
					curTime
	}

	def formattedFileSuffixTimeStamp() : String = {
			val today = Calendar.getInstance().getTime
					val formatted = new SimpleDateFormat("MMddyyHHmmss")
					val curTime = formatted.format(today)
					curTime
	}

	def random10DigitNumGenerator() : String = {
			val rand =  f"${(nextInt(26))}${nextInt(1000000000)}%04d"
			rand
	}

	def random5DigitNumGenerator() : String = {
			val rand =  f"${(nextInt(26))}${nextInt(10000)}%04d"
			rand
	}
	/**
	 * @param input
	 * @return
	 */
	def isNullOrEmpty(input : String) : Boolean =  {  
			if (input == null || input.trim.isEmpty)
				true
				else
					false
	}

	/**
	 * @param arrCol
	 * @param strVal
	 * @return
	 */
	def getArrayNotEqualsString(arrCol : Array[String],strVal:String) : Array[String] = {
			arrCol.flatMap(f => {
				if(f.replace("[", "").replace("]", "").equalsIgnoreCase(strVal)) {
					Array("")
				} else {
					Array(f)
				}
			}).filter(f => f != None)

	}

	def getArrayContainsOnlyString(arrCol : Array[String],strVal:String) : Array[String] = {
			arrCol.flatMap(f => {
				if(f.toLowerCase().replace("[", "").replace("]", "").contains(strVal.toLowerCase())) {
					Array(f)
				} else {
					Array("")
				}
			}).filter(f => f != None)
	}

	/**
	 * @param arr
	 * @param column
	 * @return
	 */
	def ifArrayContainsColumn(arr : Array[String],column:String) : Boolean = {
			arr.flatMap(f => {
				if(f.toLowerCase().replace("[", "").replace("]", "").contains(column.toLowerCase())) {
					Array(true)
				} else {
					Array(false)
				}
			}).exists(b => b)

	}


	/**
	 * @param arr
	 * @param column
	 * @return
	 */
	def ifArrayEqualsColumn(arr : Array[String],column:String) : Boolean = {
			arr.flatMap(f => {
				if(f.toLowerCase().replace("[", "").replace("]", "").equalsIgnoreCase(column.toLowerCase())) {
					Array(true)
				} else {
					Array(false)
				}
			}).exists(b => b)

	}


	/**
	 * @param columnsName
	 * @param hiveTable
	 * @param hbaseTable
	 * @param columnFamily
	 * @return
	 */
	def columnsToDDL(columnsName : Array[String], hiveTable: String,hbaseTable: String, columnFamily: String): String = {
			val columns = columnsName.map { field =>
			"  `" + field + "` " + "STRING"
			}
			val hbasecols = columnsName.map { field =>
			columnFamily + ":" + field
			}
			val DDL =  s"CREATE EXTERNAL TABLE $hiveTable (key string,\n${columns.mkString(",\n")}\n) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("+"\"hbase.columns.mapping\" = \":key,"+ {hbasecols.mkString(",\n")}+"\") TBLPROPERTIES(\"hbase.table.name\" = "+"\""+hbaseTable+"\""+", \"hbase.mapred.output.outputtable\" = "+"\""+hbaseTable+"\""+")"
					println(DDL)
			DDL
	}

	/**
	 * @param rawdf
	 * @return
	 */
	def isNestedArray(rawdf: DataFrame): Boolean = {
			rawdf.schema.fields.flatMap(field => {
				field.dataType match {
				case arrayType: ArrayType => {
					Array(true)
				}
				case _ => {
					Array(false)
				}
				}
			}).exists(b => b)
	}
	/**
	 * @param dfSchema
	 * @param prefix
	 * @return
	 */
	def flattenSchema(dfSchema: StructType, prefix: String = null): Array[Column] = {
			dfSchema.fields.flatMap(field => {
				val columnName = if (prefix == null) field.name else prefix + "." + field.name
						field.dataType match {
						case arrayType: ArrayType => {
							Array[Column](explode(col(columnName)).as(columnName.toLowerCase().replace(".", "_")))
						}
						case structType: StructType => {
							flattenSchema(structType, columnName)
						}
						case _ => {
							val columnNameWithUnderscores = columnName.toLowerCase().trim.replace(".", "_")
									Array(col(columnName).as(columnNameWithUnderscores))
						}
				}
			}).filter(field => field != None)
	}

	/**
	 * @param propertyFilePath
	 * @param conf
	 * @return
	 */
	def readproperty(propertyFilePath:String,conf:Configuration) :Properties  ={
			try{
				val props = readAsProperties(propertyFilePath,conf)
						if (props.isEmpty()) {
							println("Properties File is Empty")
						}
				return props
			}
			catch {
			case e: Exception =>
			e.printStackTrace()
			sys.exit(1)
			}
	}

	/**
	 * @param path
	 * @param conf
	 * @return
	 */
	def readAsProperties(path: String,conf:Configuration): Properties = {
			val properties = new Properties()
					val pt = new Path(path)
					val fs = pt.getFileSystem(conf)
					val stream = fs.open(pt)
					properties.load(stream)
					properties
	}

	/**
	 * @param conf
	 * @param filePathName
	 * @return
	 */
	def ifFileExistsOnHDFS(conf:Configuration,filePathName : String) : Boolean = {
			val filePath = new Path(filePathName)
					val fs = filePath.getFileSystem(conf)
					var fileExists = false
					if (fs.exists(filePath)) {
						fileExists = true
					}
			fileExists
	}

	//LoadID Generation
	/**
	 * @param randNumber
	 * @return
	 */
	def randomLoadIDGenerator(randNumber : String) : String = {
			var randNo = randNumber
					if (randNo.isEmpty() || randNo == "") {

						randNo =  f"${(nextInt(26))}${nextInt(10000)}%04d"
						randNo
					} else {

						var randNoInt = randNo.toInt + 1
								randNo = randNoInt.toString()
								randNo
					}
	} 

	def processStartTime () : String = {
			//Start Time for Audit
			val currentDetTimeStart = System.currentTimeMillis();
			val detStart = new Date(currentDetTimeStart);
			var detStartTime = new SimpleDateFormat("HH:mm:ss").format(detStart.getTime()) 
					detStartTime
	}

	def processEndTime () : String = {
			//End Time for Audit
			val currentDetTimeEnd = System.currentTimeMillis()
					val detEnd = new Date(currentDetTimeEnd)
					var detEndTime = new SimpleDateFormat("HH:mm:ss").format(detEnd.getTime())
					detEndTime
	}

	def statsLoadDetails(hiveContext:HiveContext,readProp:Properties) : String = {
			val maxrandQuery = "select load_id from " + readProp.getProperty(DataIngestionConstants.AUDIT_DATABASE) + "." +  
					readProp.getProperty(DataIngestionConstants.REAL_TIME_STATS_TABLE) + " order by load_id desc limit 1" 

					//hiveContext.clearCache()
					hiveContext.refreshTable(readProp.getProperty(DataIngestionConstants.AUDIT_DATABASE) + "." +readProp.getProperty(DataIngestionConstants.REAL_TIME_STATS_TABLE))
					var maxrandNoDF = hiveContext.sql(maxrandQuery)
					var loadID : String = ""

					//Load ID Generation
					if (maxrandNoDF.count() < 1)
					{
						loadID = DataIngestionUtility.randomLoadIDGenerator("")
					}
					else
					{
						var maxrandNo = maxrandNoDF.select("load_id").collect().toSeq.map(x => x.toString).head.replace("[", "").replace("]", "")
								loadID = DataIngestionUtility.randomLoadIDGenerator(maxrandNo)
					} 
					loadID
	}

	/** To extract Column Details from the Dataframe based on the Column defined in the Schema file.
	 * @param schemaColumnsMap
	 * @return columnMatchName
	 */
	def getColumnNames(schemaColumnsMap:scala.collection.mutable.LinkedHashMap[String,String],columnMatchName:String) : String = {
			var actualColumn : String = ""
					var publisherName : String = ""
					if (schemaColumnsMap.filter(p => p._2.toLowerCase().contains(columnMatchName)).nonEmpty) {
						actualColumn = schemaColumnsMap.filter(p => p._2.toLowerCase().contains(columnMatchName)).keySet.head

					}
	actualColumn
	}

	/** To get the JSON Strings from the DataFrame
	 * @param columnName
	 * @param rawJSONDF
	 * @return
	 */
	def returnJSONdatafromRawJSON(columnName: String, rawJSONDF : DataFrame ): String = {
			if (rawJSONDF.select(columnName).toString().split(":")(1).contains("string]") )
			{
				val JSONString = rawJSONDF.select(columnName).toJSON
						val splitString = "\\{\"" + columnName + "\":"
						val outStr = JSONString.collect().map { x => if (x.length() >2 ) {x.split(splitString)(1)}  }
				outStr(0).toString.replace("}", "")
			}
			else
			{
				val JSONString = rawJSONDF.select(columnName).toJSON
						val splitString = "\\{\"" + columnName + "\":"
						val outStr = JSONString.collect().map { x => x.split(splitString)(1)  }
				outStr(0)
			}
	}


	/** To move the part files as single JSON file
	 * @param conf
	 * @param srcFilePathName
	 * @param desFilePathName
	 */
	def moveFileOnHDFS(conf:Configuration,srcFilePathName : String,desFilePathName:String)  = {
			val srcfilePath = new Path(srcFilePathName)
					try {
						val fs = srcfilePath.getFileSystem(conf)
								if (fs.exists(srcfilePath)) {
									// FileUtil.copyMerge(fs, srcfilePath, desfilePath.getFileSystem(conf), desfilePath, false, conf, fileName)
									val result = "hadoop fs -mv "+srcFilePathName+" "+ desFilePathName !!

											println(result)
								}
					} catch {
					case e: Exception =>
					{
						println("Could not move " + srcFilePathName +" "+ desFilePathName + e.getMessage)
						e.printStackTrace()
					}
					}
	}

	/** To Concatenate the given list of Columns as a new column for ROW Update 
	 * @param mainDF
	 * @param concatColumns
	 * @param sqlContext
	 * @param montiorKeyCol
	 * @return
	 */
	def concatRowsForNewColumn(mainDF : DataFrame,concatColumns:Array[String],sqlContext : SQLContext,montiorKeyCol:String) : DataFrame = {
			var newConcatDF : DataFrame = sqlContext.emptyDataFrame
					newConcatDF = mainDF.withColumn(montiorKeyCol, (concat_ws("_",concatColumns.map(c => col(c)): _*)))
					newConcatDF
	}


	/**
	 * @param conf
	 * @param filePathName
	 * @param content
	 */
	def fileAppendHDFS(conf:Configuration,filePathName : String,content:String) = {
			val hdfsPath = new Path(filePathName)
					conf.setBoolean("fs.hdfs.impl.disable.cache", true)
					val fileSystem = hdfsPath.getFileSystem(conf)

					var fileOutputStream : FSDataOutputStream = null
					try {
						if (fileSystem.exists(hdfsPath)) {
							// println("*********************************File Append*****************************")
							//println(content)
							//fileSystem.open(hdfsPath)
							fileOutputStream = fileSystem.append(hdfsPath)
									fileOutputStream.writeBytes("\n"+content)
						}
					} catch {
					case e: Exception =>
					{
						println("Could not append to file " + filePathName +" " + e.getMessage)
						e.printStackTrace()
					}
					} finally {
						if (fileSystem != null) {
							fileSystem.close()
						}
						if (fileOutputStream != null) {
							fileOutputStream.close()
						}
					}
	}
	
		/**
		 * @param df
		 * @param requiredColumn
		 * @return
		 */
		def notNullColumns(df:DataFrame,requiredColumn:Array[String]) : Array[String] = {
			//	requiredColumn.foreach { f =>
			requiredColumn.flatMap(f => {
				var actualCol = f.trim
						val mandateDF = Try(df.select(actualCol)).toOption
						if (mandateDF.exists(p => true)) {
							if (! df.select(actualCol).head().anyNull) {
								Array(actualCol)
							} else {
								Array("")
							}
						}else {
							Array("")
						}
			}).filter(f => f != None)
	}

}