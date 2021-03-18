package com.humira.utilities

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{array, concat, lit, struct}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import unicredit.spark.hbase.toHBaseRDDSimple
import unicredit.spark.hbase.toHBaseSC
import unicredit.spark.hbase.HBaseConfig
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Get,Put, HBaseAdmin, HTable}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.BinaryComparator
import java.util.Properties

import com.humira.constants.DataIngestionConstants
import com.humira.utilities.DataIngestionUtility

/**
 * @author CTS Commercial Datalake Team
 *This Object HBaseUtility is a reusable Component with common HBase Functionality
 * 1. loadUpdateDatatoHBase() is used to load/Update DataFrame RDD as (Key/Value) into HBase based ROW Key Existence
 * 2. loadUpdateROWtoHBase()  is used to load/Update DataFrame RDD as (Key/Value) into HBase based ROW Key & Specific Column Existence
 * 3. flattenedArrayDatatoHBase() is used to load DataFrame RDD as (Key/Value) (insert-mode) into HBase
 * 4. createHbaseHistoryTable() is used to load RDD as (Key/Value) from Current Base table into History HBase Table 
 * 5. checkIfRowKeyExists() is used to Check If ROW Key Exists in the given HBase Table
 * 6. createHBaseTable() is used to Create HBase Table if not exists
 * 7. addColumnFamily() is used to create HBase Column Family if not exists
 * 8. ifHBaseTableExists() is used to Check If Table Exists
 * 9. exceptionDatatoHBase() is used to load Exception Table
 * 10. checkIfEntireRowExists() is used to Check if given Column Value exists
 */
object HBaseUtility {


	/** To Load/Update the HBase Table with the DataFrame based on the existence of the ROW Key
	 * @param final_df
	 * @param curTime
	 * @param sc
	 * @param table
	 * @param columnFamily
	 * @param hb
	 * @param admin
	 * @param hbasedatabaseName
	 * @return
	 */
	def loadUpdateDatatoHBase(final_df : DataFrame,curTime : String,sc : SparkContext,table:String,columnFamily:String,hb : Configuration,admin: HBaseAdmin,
			hbasedatabaseName : String,readProp: Properties) : Boolean = {
					var jobStatus : Boolean = false
							implicit val hbconfig = HBaseConfig()
							val hbaseTable = hbasedatabaseName+":"+table
							val loadTime = DataIngestionUtility.formattedKeyTimeStamp()

							if (final_df.count() > 0) {
								val mainDF = final_df.na.fill("")
										val dfColumnNames = mainDF.columns
										val keyValueSeq = mainDF.select(dfColumnNames.head).collect().map(f => f.toString()).toSeq

										if(keyValueSeq.size > 0) {
											if (admin.tableExists(hbaseTable)) {
												for(keyValue <- keyValueSeq) {
													var rowKeyVal = keyValue.trim.replace("[", "").replace("]", "")

															val keyExists = checkIfRowKeyExists(hb,admin,hbaseTable,columnFamily,rowKeyVal) // To Check if the Row key exists in Given HBase Table

															val dfMapVal = mainDF.collect.flatMap(r => 
															Map(
																	r(0).toString->Map(mainDF.columns.zip(r.toSeq):_*).mapValues(_.toString.replace("WrappedArray(", ""))
																	)).toSeq
															val dfRDD = sc.parallelize(dfMapVal)
															if (keyExists) {
															//	createHBaseTable(admin,hbaseTable,columnFamily) // To Create the HBase table if not exists
															//	addColumnFamily(admin,hbaseTable,columnFamily) // To add the Column family if not exists
																jobStatus = true
																val htable = new HTable(hb, hbaseTable)

																val putVal = new Put(Bytes.toBytes(rowKeyVal))  
																dfRDD.collect().map {
																case (k, v) =>
																if(k.equalsIgnoreCase(rowKeyVal)) {
																	v.keySet.foreach(f =>
																	if (! DataIngestionUtility.isNullOrEmpty(f) &&  ! DataIngestionUtility.isNullOrEmpty(v(f))) {
																		if ( !f.equalsIgnoreCase(readProp.getProperty(DataIngestionConstants.COL_CREATED_DATE)) && !f.equalsIgnoreCase(readProp.getProperty(DataIngestionConstants.COL_CREATED_BY))) {
																			putVal.add(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(v(f).replace("WrappedArray(", "")))
																		}
																	})
																}
																}
																htable.put(putVal)
																htable.close()
															}
															else {
																createHBaseTable(admin,hbaseTable,columnFamily) // To Create the HBase table if not exists
																addColumnFamily(admin,hbaseTable,columnFamily) // To add the Column family if not exists
																jobStatus = true
																val htable = new HTable(hb, hbaseTable)
																val putVal = new Put(Bytes.toBytes(rowKeyVal))  
																dfRDD.collect().map {
																case (k, v) =>
																if(k.equalsIgnoreCase(rowKeyVal)) {
																	v.keySet.foreach(f =>
																	putVal.add(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(v(f).replace("WrappedArray(", "")))
																			)
																}
																}
																htable.put(putVal)
																htable.close()
															}

													createHbaseHistoryTable(loadTime,sc,admin,hbasedatabaseName,table,columnFamily,rowKeyVal) // To create/Load data into History table from existing current HBase Table
												}
											} else { // If the HBase Table does not exists
												val dfMapVal = mainDF.collect.flatMap(r => 
												Map(
														r(0).toString->Map(mainDF.columns.zip(r.toSeq):_*).mapValues(_.toString)
														)).toSeq

														val dfRDD = sc.parallelize(dfMapVal)
														createHBaseTable(admin,hbaseTable,columnFamily)
														addColumnFamily(admin,hbaseTable,columnFamily)
														jobStatus = true

														dfRDD.toHBase(hbaseTable, columnFamily) //		println("Written to HBase")

														createHbaseHistoryTable(loadTime,sc,admin,hbasedatabaseName,table,columnFamily,keyValueSeq.head) // To create/Load data into History table from existing current HBase Table
											}
										} 

							}
	jobStatus
	}


	/** To Load/Update the HBase Table with the DataFrame based on the existence of the ROW with specific Column value combination which is concatenated 
	 *  and stored as a single column for Comparison during Update
	 * @param final_df
	 * @param curTime
	 * @param sc
	 * @param table
	 * @param columnFamily
	 * @param hb
	 * @param admin
	 * @param hbasedatabaseName
	 * @param monitorKeyCol
	 * @return
	 */
	def loadUpdateROWtoHBase(final_df : DataFrame,curTime : String,sc : SparkContext,table:String,columnFamily:String,hb : Configuration,admin: HBaseAdmin,
			hbasedatabaseName : String,monitorKeyCol:String,readProp: Properties) : Boolean = {
					var jobStatus : Boolean = false
							implicit val hbconfig = HBaseConfig()
							val hbaseTable = hbasedatabaseName+":"+table
							//		println("hbase table : "+hbaseTable)
							val loadTime = DataIngestionUtility.formattedKeyTimeStamp()

							if (final_df.count() > 0) {
								val mainDF = final_df.na.fill("")
										val dfColumnNames = mainDF.columns
										val keyValueSeq = mainDF.select(dfColumnNames.head).collect().map(f => f.toString()).toSeq

										val monitorROWSeq = mainDF.select(monitorKeyCol).collect().map(f => f.toString()).toSeq

										keyValueSeq.foreach(println)

										if(keyValueSeq.size > 0) {
											if (admin.tableExists(hbaseTable)) {
												for(keyValue <- keyValueSeq) {
													var rowKeyVal = keyValue.trim.replace("[", "").replace("]", "")

															val keyExists = checkIfRowKeyExists(hb,admin,hbaseTable,columnFamily,rowKeyVal) 	// To Check if the Row key exists in Given HBase Table

															val dfMapVal = mainDF.collect.flatMap(r => 
															Map(
																	r(0).toString->Map(mainDF.columns.zip(r.toSeq):_*).mapValues(_.toString.replace("WrappedArray(", ""))
																	)).toSeq
															val dfRDD = sc.parallelize(dfMapVal)
															if (keyExists) {
																if(monitorROWSeq.size > 0) {
																	for(monitorVal <- monitorROWSeq) {
																		var monitorRowVal = monitorVal.trim.replace("[", "").replace("]", "")

																				val rowExists = checkIfEntireRowExists(sc, hb, admin, hbaseTable, columnFamily, monitorRowVal, monitorKeyCol,rowKeyVal) // To Check if the Entire Row exists based on few column combination in Given HBase Table

																				if (rowExists) {
																					jobStatus = true
																							createHBaseTable(admin,hbaseTable,columnFamily)
																							addColumnFamily(admin,hbaseTable,columnFamily)
																							val htable = new HTable(hb, hbaseTable)
																							val putVal = new Put(Bytes.toBytes(rowKeyVal))  
																							dfRDD.collect().map {
																							case (k, v) =>
																							if(k.equalsIgnoreCase(rowKeyVal) && v(monitorKeyCol).equalsIgnoreCase(monitorRowVal)) {
																								v.keySet.foreach(f =>
																								if (! DataIngestionUtility.isNullOrEmpty(f) &&  ! DataIngestionUtility.isNullOrEmpty(v(f))) {
																									if ( !f.equalsIgnoreCase(readProp.getProperty(DataIngestionConstants.COL_CREATED_DATE)) && !f.equalsIgnoreCase(readProp.getProperty(DataIngestionConstants.COL_CREATED_BY))) {
																										putVal.add(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(v(f).replace("WrappedArray(", "")))
																									}
																								})
																								htable.put(putVal)

																							}
																					}
																					htable.close()

																				} else {
																					jobStatus = true
																							createHBaseTable(admin,hbaseTable,columnFamily)
																							addColumnFamily(admin,hbaseTable,columnFamily)
																							val htable = new HTable(hb, hbaseTable)
																							val putVal = new Put(Bytes.toBytes(rowKeyVal))  
																							dfRDD.collect().map {
																							case (k, v) =>
																							if(k.equalsIgnoreCase(rowKeyVal) && v(monitorKeyCol).equalsIgnoreCase(monitorRowVal)) {
																								v.keySet.foreach(f =>
																								if (! DataIngestionUtility.isNullOrEmpty(f) &&  ! DataIngestionUtility.isNullOrEmpty(v(f))) {
																									//if ( !f.equalsIgnoreCase(readProp.getProperty(DataIngestionConstants.COL_CREATED_DATE)) && !f.equalsIgnoreCase(readProp.getProperty(DataIngestionConstants.COL_CREATED_BY))) {
																									putVal.add(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(v(f).replace("WrappedArray(", "")))
																									//	}
																								})

																								htable.put(putVal)

																							}
																					}
																					htable.close()

																				}
																	}
																}              
															}
															else {
																createHBaseTable(admin,hbaseTable,columnFamily)
																addColumnFamily(admin,hbaseTable,columnFamily)
																jobStatus = true
																val htable = new HTable(hb, hbaseTable)
																val putVal = new Put(Bytes.toBytes(rowKeyVal))  
																dfRDD.collect().map {
																case (k, v) =>
																if(k.equalsIgnoreCase(rowKeyVal)) {
																	v.keySet.foreach(f =>
																	putVal.add(Bytes.toBytes(columnFamily), Bytes.toBytes(f), Bytes.toBytes(v(f).replace("WrappedArray(", "")))
																			)
																	htable.put(putVal)
																}
																}

																htable.close()
															}

													createHbaseHistoryTable(loadTime,sc,admin,hbasedatabaseName,table,columnFamily,rowKeyVal) 	// To create/Load data into History table from existing current HBase Table
												}
											} else { 	// If the HBase Table does not exists

												val dfMapVal = mainDF.collect.flatMap(r => 
												Map(
														r(0).toString->Map(mainDF.columns.zip(r.toSeq):_*).mapValues(_.toString)
														)).toSeq

														val dfRDD = sc.parallelize(dfMapVal)
														createHBaseTable(admin,hbaseTable,columnFamily)
														addColumnFamily(admin,hbaseTable,columnFamily)
														jobStatus = true
														dfRDD.toHBase(hbaseTable, columnFamily)

														createHbaseHistoryTable(loadTime,sc,admin,hbasedatabaseName,table,columnFamily,keyValueSeq.head) // To create/Load data into History table from existing current HBase Table
											}
										} 

							}
	jobStatus
	}


	/** To Load the given dataFrame into HBase - Used for Patient Interactions 
	 * @param final_df
	 * @param sc
	 * @param columnFamily
	 * @param admin
	 * @param hbaseTable
	 */
	def flattenedArrayDatatoHBase(final_df : DataFrame,sc : SparkContext,columnFamily:String,admin: HBaseAdmin,hbaseTable : String) = {
			implicit val hbconfig = HBaseConfig()
					if (final_df.count() > 0) {
						val mainDF = final_df.na.fill("")
								val dfMapVal = mainDF.collect.flatMap(r => 
								Map(
										r(0).toString->Map(mainDF.columns.zip(r.toSeq):_*).mapValues(_.toString)
										)).toSeq

								val dfRDD = sc.parallelize(dfMapVal)
								//println("*****************************************Creating and Loading data into Interactions Hbase***********************************")
								createHBaseTable(admin,hbaseTable,columnFamily)
								addColumnFamily(admin,hbaseTable,columnFamily)
								dfRDD.toHBase(hbaseTable, columnFamily) 	//println("Written to HBase")
					}
	}

	/** Loading data from Current HBase tables into History tables for given Row Key
	 * @param curTime
	 * @param sc
	 * @param admin
	 * @param hbasedatabaseName
	 * @param tableName
	 * @param familyName
	 * @param rowKey
	 */
	def createHbaseHistoryTable(curTime : String,sc : SparkContext,admin: HBaseAdmin, hbasedatabaseName : String,tableName: String, familyName: String, rowKey : String) = {
			val families = Set(familyName)
					val hbaseHistoryTable = hbasedatabaseName+":"+tableName+"_history"
					implicit val hbconfig = HBaseConfig()
					if (admin.tableExists(hbasedatabaseName+":"+tableName)) {

						val scan: Scan = new Scan()
								scan.setStartRow(Bytes.toBytes(rowKey))
								scan.setStopRow(Bytes.toBytes(rowKey))

								val rdd = sc.hbase[String](hbasedatabaseName+":"+tableName, families, scan)

								val neededrdd = rdd.collect().map {
								case (k, v) =>
								val cf = v(familyName)
								(k+"_"+curTime, cf)
					}

			val historyRDD = sc.parallelize(neededrdd)
					//println("*****************************************Creating and Loading data into History Hbase***********************************")
					createHBaseTable(admin,hbaseHistoryTable,familyName+"_history")
					addColumnFamily(admin,hbaseHistoryTable,familyName+"_history")
					historyRDD.toHBase(hbaseHistoryTable, familyName+"_history")
					}
	}



	/** Check If ROW Key exists in the HBase Table
	 * @param hb
	 * @param admin
	 * @param tableName
	 * @param familyName
	 * @param rowKey
	 * @return
	 */
	def checkIfRowKeyExists(hb : Configuration,admin: HBaseAdmin, tableName: String, familyName: String, rowKey : String) : Boolean = {
			var keyExists : Boolean = false
					var cfExists : Boolean = false
					//				println("CHECK if ROW Key Exists for : "+tableName)
					//				println("col Family : "+familyName)
					//				println("rowKey : "+rowKey)
					if (admin.tableExists(tableName)) {
						val htable = new HTable(hb, tableName)
								htable.getTableDescriptor.getColumnFamilies.foreach(fCol =>
								if (fCol.getNameAsString.equalsIgnoreCase(familyName))
								{
									cfExists = true
								})
								if(cfExists) {
									val rowKeyVal = htable.get(new Get(Bytes.toBytes(rowKey)).addFamily(familyName.getBytes)).toString()
											if (! rowKeyVal.isEmpty()) {
												if (rowKeyVal.contains(rowKey)) {

													keyExists = true
												}
											}
								}
					}
	keyExists
	}


	/** To create HBase table is not exists
	 * @param admin
	 * @param tableName
	 * @param familyName
	 */
	def createHBaseTable(admin: HBaseAdmin, tableName: String, familyName: String): Unit = {
			val tableDescriptor = new HTableDescriptor(tableName)
					val colDescriptor   = new HColumnDescriptor(familyName)
					tableDescriptor.addFamily(colDescriptor)
					if (!admin.tableExists(tableName)) {
						admin.createTable(tableDescriptor)
					}
	}
	/** To create column family for HBase Table if not exists
	 * @param admin
	 * @param tableName
	 * @param familyName
	 */
	def addColumnFamily(admin: HBaseAdmin, tableName: String, familyName: String) = {
			val colDescriptor   = new HColumnDescriptor(familyName)
					var exists = false
					if (admin.tableExists(tableName)) {
						admin.listTables(tableName).foreach(f =>
						f.getColumnFamilies.foreach(fCol =>
						if (fCol.getNameAsString.equalsIgnoreCase(familyName))
						{
							exists = true
						}))
						if(! exists) {
							admin.addColumn(tableName, colDescriptor)
						}
					}
	}

	/** To check if HBase Table exists
	 * @param admin
	 * @param tableName
	 * @param familyName
	 */
	def ifHBaseTableExists(admin: HBaseAdmin, tableName: String): Boolean = {
			var tableExists : Boolean = false
					if (admin.isTableAvailable(tableName)) {
						tableExists = true
					}
	tableExists
	}

	/** To Load the exception/invalid data into exception Hbase Table
	 * @param final_df
	 * @param curTime
	 * @param sc
	 * @param columnFamily
	 * @param admin
	 * @param hbaseTable
	 */
	def exceptionDatatoHBase(final_df : DataFrame,curTime:String,sc : SparkContext,columnFamily:String,admin: HBaseAdmin,hbaseTable : String) = {
			implicit val hbconfig = HBaseConfig()
					if (final_df.count() > 0) {

						val mainDF = final_df.na.fill("")
								val selectColumns = mainDF.columns.map(fieldVal => lit(DataIngestionUtility.returnJSONdatafromRawJSON(fieldVal, mainDF)).as(fieldVal))
								val exceptionDF = mainDF.select(selectColumns :_*)
								val dfMapVal = exceptionDF.collect.map(r => 
								Map(
										curTime.toString.replace(" ", "")->Map(exceptionDF.columns.zip(r.toSeq):_*).mapValues(_.toString)
										)).toSeq

								val dfRDD = sc.parallelize(dfMapVal.flatMap(f => f))
								//println("*****************************************Creating and Loading data into Exception records Hbase***********************************")
								createHBaseTable(admin,hbaseTable,columnFamily)
								addColumnFamily(admin,hbaseTable,columnFamily)
								dfRDD.toHBase(hbaseTable, columnFamily)
					}
	}

	/** To encode the Scan Object and return the String
	 * @param scan
	 * @return
	 */
	def convertScanToString(scan : Scan): String = { 
			val proto: org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan  = ProtobufUtil.toScan(scan)
					Base64.encodeBytes(proto.toByteArray()) 
	} 	

	/** To Check if the specific Column value exists based on the filter applied on the Column if the ROW Key exists. 
	 * @param sc
	 * @param hb
	 * @param admin
	 * @param tableName
	 * @param familyName
	 * @param rowVal
	 * @param monitorKeyCol
	 * @param rowKey
	 * @return
	 */
	def checkIfEntireRowExists(sc : SparkContext,hb : Configuration,admin: HBaseAdmin, tableName: String, familyName: String, rowVal : String,monitorKeyCol:String,rowKey : String) : Boolean = {
			var rowExists : Boolean = false
					//			println("CHECK if ROW Exists for : "+tableName)
					//			println("col Family : "+familyName)
					//			println("rowVal : "+rowVal)
					if (admin.tableExists(tableName)) {
						val scan: Scan = new Scan()

								hb.set(TableInputFormat.INPUT_TABLE, tableName)
								hb.set(TableInputFormat.SCAN_COLUMNS, familyName) // scan data column family

								val filterList : FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL) 	//Filter to pass all the conditions

								val filter: SingleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(familyName),Bytes.toBytes(monitorKeyCol), CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(rowVal)))

								filter.setFilterIfMissing(true) // To Set the Filter to return records only if column value found  
								filter.setLatestVersionOnly(true) // To Set the Filter to check only the latest version

								filterList.addFilter(filter)

								scan.setStartRow(Bytes.toBytes(rowKey))
								scan.setStopRow(Bytes.toBytes(rowKey)) // To Start/End the Scan on ROW Key

								scan.setFilter(filterList)

								hb.set(TableInputFormat.SCAN, convertScanToString(scan))

								val hBaseRDD = sc.newAPIHadoopRDD(hb, classOf[TableInputFormat],
										classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
										classOf[org.apache.hadoop.hbase.client.Result]) // Load an RDD of row key, result(ImmutableBytesWritable, Result) tuples from the table

								if(hBaseRDD.count() > 0) {
									//	hBaseRDD.collect().map(p => p._2).foreach(println)
									rowExists = true
								}
					}
	rowExists
	}

}