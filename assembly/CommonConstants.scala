package com.humira.constants

object CommonConstants {

  final val SETHIVEVAR = "SET hivevar:db="
  final val SPACE = " "
  final val DRIVERLAYERNAME = "refinedLayer1"
  final val DRIVERLAYER1NAME = "Layer1"
  final val COMMONVAR1 = "hiveCommonVar1"
  final val COMMONVAR2 = "hiveCommonVar2"
  final val COMMONVAR3 = "hiveCommonVar3"
  final val COMMONVAR4 = "hiveCommonVar4"
  final val FILE_SEPARATOR = "/"

  final val CREATEDATABASEIFNOTEXISTS_RAW = "Raw.QUERY1"
  final val CREATEEXTERNALTABLE_RAW = "Raw.QUERY2"
  final val ALTERTABLE_RAW = "Raw.QUERY3"
  final val UPDATETBLPARTITIONS_RAW = "Raw.QUERY4"
  final val CREATEAUDITTABLE_RAW = "Raw.QUERY5"
  final val RECORDCOUNT_RAW = "Raw.QUERY6"
  final val CREATEEXTERNALTABLE_RAW_P1 = "Raw.QUERY7"
  final val ALTERTABLE_RAW_P1 = "Raw.QUERY8"
  final val UPDATETBLPARTITIONS_RAW_P1 = "Raw.QUERY9"
  final val CREATEEXTERNALTABLE_RAW_P2 = "Raw.QUERY10"
  final val ALTERTABLE_RAW_P2 = "Raw.QUERY11"
  final val UPDATETBLPARTITIONS_RAW_P2 = "Raw.QUERY12"

  final val RAWTBLDELIMITER = "DELIMITER"
  final val RAWTBLNAME = "Raw.hiveVar1"
  final val RAWTBLLOCATION = "Raw.hiveVar2"
  final val RAWTBLSCHEMA = "Raw.hiveVar3"
  final val RAWPARTITIONCOL = "Raw.hiveVar4"
  final val RAWNEWSCHEMALOCATION = "Raw.hiveVar5"
  final val AUDITTBLNAME = "Raw.hiveVar6"
  final val AUDITTABLELOCATION = "Raw.hiveVar7"
  final val AUDITTBLSCHEMA = "Raw.hiveVar8"
  final val AUDITTABLEFILE = "Raw.hiveVar9"
  final val RAWTBLNAME_P1 = "Raw.hiveVar10"
  final val RAWTBLLOCATION_P1 = "Raw.hiveVar11"
  final val RAWTBLNAME_P2 = "Raw.hiveVar12"
  final val RAWTBLLOCATION_P2 = "Raw.hiveVar13"
  //final val RAWTBLLOCATION_ENC="Raw.hiveVar15"
  final val ENCSTGTABLLAND = "Raw.hiveVar15"
  final val RAWTBLNAMEENC = "Raw.hiveVar16"

  final val LASTRUNTABLENAME = "PersistTable.PersistenceLastRunTbl"

  final val REFINEDPROPS = "RefinedPropertyLocation"
  final val UPDATETBLPARTITIONS_REFINED_VIEW = "Refined.QUERY5"

  final val CREATEDATABASEIFNOTEXISTS_RFD = "Refined.QUERY1"
  final val CREATEEXTERNALTABLE_RFD = "Refined.QUERY2"
  final val UPDATETBLPARTITIONS_REFINED = "Refined.QUERY3"
  final val CREATEEXTERNALTABLE_RFD_VIEW = "Refined.QUERY4"
  //final val UPDATETBLPARTITIONS_REFINED_VIEW = "Refined.QUERY5"
  final val ALTERTABLE_RFD = "Refined.QUERY6"
  final val ALTERTABLE_RFD_VIEW = "Refined.QUERY5"
  final val INSERTINTOTABLE_RFD_VIEW = "Refined.QUERY8"
  final val RECORDCOUNT_RFD = "Refined.QUERY7"

  final val CREATEEXTERNALTABLE_RFD_P1 = "Refined.QUERY9"
  final val ALTERTABLE_RFD_P1 = "Refined.QUERY10"
  final val UPDATETBLPARTITIONS_RFD_P1 = "Refined.QUERY11"
  final val CREATEEXTERNALTABLE_RFD_P2 = "Refined.QUERY12"
  final val ALTERTABLE_RFD_P2 = "Refined.QUERY13"
  final val UPDATETBLPARTITIONS_RFD_P2 = "Refined.QUERY14"

  final val RFDTBLNAME = "Refined.hiveVar1"
  final val RFDTBLNAME_VIEW = "Refined.hiveVar6"
  final val RFDTBLLOCATION = "Refined.hiveVar2"
  //final val RFDNEWSCHEMALOCATION = "Refined.hiveVar11"
  final val RFDTBLSCHEMA = "Refined.hiveVar7"
  final val RFDPARTITIONCOL = "Refined.hiveVar4"
  // final val RFDNEWSCHEMALOCATION_VIEW = "Refined.hiveVar10"
  final val RFDTBLDELIMITER = "DELIMITER"
  final val RFDTBLSCHEMA_VIEW = "Refined.hiveVar3"
  final val RFDTBLSELECT_COLS = "Refined.hiveVar8"
  //final val RFDTBLCNTLOCATION = "Refined.hiveVar12"
  //  final val OUTPUTFILEPATH_RFD = "Refined.hiveVar13"
  final val RFDTBLNAME_P1 = "Refined.hiveVar10"
  final val RFDTBLLOCATION_P1 = "Refined.hiveVar11"
  final val RFDTBLNAME_P2 = "Refined.hiveVar12"
  final val RFDTBLLOCATION_P2 = "Refined.hiveVar13"
  final val RFDNEWSCHEMASTRING = "Raw.hiveVar5"
  final val RFDNEWSCHEMAACTUAL = "Refined.hiveVar15"
  final val AUDITFILEPATH = "Raw.hiveVar9"
  

  // schemaTableCreation constants
  final val CREATEDATABASEIFNOTEXISTS = "Schema.QUERY1"
  final val CREATEEXTERNALTABLE_ACTUAL = "Schema.QUERY2"
  final val CREATEEXTERNALTABLE_STRING = "Schema.QUERY3"
  final val SCHEMADBNAME = "Schema.hiveVar1"
  final val SCHEMAACTUALTBLNAME = "Schema.hiveVar2"
  final val SCHEMASTRINGTBLNAME = "Schema.hiveVar3"
  final val SCHEMAACTUALTBLLOCATION = "Schema.hiveVar4"
  final val SCHEMASTRINGTBLLOCATION = "Schema.hiveVar5"
  final val SCHEMAACTUALLOCATION = "Schema.hiveVar6"
  final val SCHEMASTRINGLOCATION = "Schema.hiveVar7"

  final val DRIVERCOPYLANDINGTORAW = "LandingToRaw"
  //final val MASTERSCHEMAPATH = "MasterSchemaPath"
  final val MASTERSCHEMA = "masterSchema"
  final val DETAILEDCONTROLTABLEDB = "DetailedControlTableDB"
  final val DETAILEDCONTROLTABLENAME = "DetailedControlTableName"
  final val FILELISTCURRENTABSOLUTEPATH = "ListFileCurrentLoadDir"
  final val LANDINGBASEPATH = "LandingBasePath"
  final val LINEAGEFILEABSOLUTEPATH = "LineageFileAbsolutePath"
  final val AUDITFILEBASEPATH = "AuditFileBasePath"
  final val FILENAMEPATTERNREFFILE = "FileNamePatternRefFile"
  final val BACKUPREFINEDPATH = "BackupRefinedPath"
  final val INCOMINGSCHEMAFILE = "incomingSchemaFile"
  final val FILELISTNAME = "FileListName"
  final val INCOMINGSCHEMAPATH = "incomingSchema"
  final val FEEDFROMPROPFILE = "PERSISTENCE.LAYER1JOB"

  //file Validation constants
  final val FILEVALIDATION = "FileValidation"
  final val RENAMEFILE = "RenameFile"
  final val DATARESTATEMENT ="DataRestatement"
  final val CONTROLFILEPATH = "CotrolFileAbsolutePath"
  final val CONTROLFILELANDINGPATH = "ControlFilePath"
  final val MARKETCODE = "MarketCode"
  final val MARKETCD = "MarketCd"
  final val DATASET = "DataSet"
  final val VENDOR = "Vendor"
  final val DATASETTBL = "DataSetTbl"
  final val DATASETFILE = "DataSetFile"
  //final val AUDITFILEBASEPATH = "AuditFileBasePath"
  final val AUDITFILEBASEPATH1 = "AuditFileBasePath1"
  //final val CREATEDETAILAUDITTABLE = "CreateDetailAuditTable"
  final val CREATEDETAILAUDITTABLE = "CreateDetailAuditTable"
  final val LOGLOCATION = "CopyScriptLog_Loc"
  final val METADATALOCATION = "MetaDataLocation"
  //final val DTLAUDITTBLDB = "hiveCommonVar2"
  final val DTLAUDITTBLDB = "AuditDBCreation"
  final val DTLAUDITTBLNAME = "Copy.hiveVar2"
  final val DTLAUDITLOC = "Copy.hiveVar3"
  final val DTLCREATETDB = "Copy.QUERY1"
  final val DTLCREATETBL = "Audit_DB"
  final val DTLSCHEMA = "Copy.hiveVar1"
  final val DTLMSCK = "AuditTblMSCKRepair"
  final val DetailedFileLocation = "DetailedFileLocation"
  final val DetailedAuditDB = "DetailedAuditDB"
  final val DetailedAuditTable = "DetailedAuditTable"
  

  final val REMOVEHEADER = "removeHeader"
  final val PARTIALREFFLAG = "PartialRefreshFlag"
  final val TRANSACTIONDATECOLNAME = "TransactionDateColName"
  final val TRANSACTIONDATEFORMAT = "TransactionDateFormat"
  final val ACTIVEFLAGCOLNAME = "ActiveFlagColName"

  //Encryption variables
  final val UDFLOCATION = "Raw.udflocation"
  final val ENCFUNCTION = "Raw.encfunction"
  final val ENCCREATETABLE = "Raw.enccreatetable"
  final val ENCINSERTTABLE = "Raw.encinserttable"
  final val ENCDROPTMPTABLE = "Raw.encdroptmptable"
  final val ENCDROPCRYPTTBL = "Raw.encdropcrypttbl"
  final val ENCMSCKOLDTBL = "Raw.encmsckoldtbl"
  final val NONENCLANDCREATETABLE = "Raw.nonenclandcreatetable"
  final val ENCLANDCREATETABLE = "Raw.enclandcreatetable"
  final val NONENCDROPLANDTABLE = "Raw.nonencdroplandtable"
  final val ENCDROPLANDTABLE = "Raw.encdroplandtable"
  final val ENCLANDINSERTINTOTBL = "Raw.enclandinsertintotable"
  final val ACTUALCOUNTLOCATION = "Actualcountlocation"
  final val feedName = "feedname"
  final val MASTERSCHEMAFILENAME = "masterSchemaFile"
  final val DELIMITERSOURCE = "FileDelimiter"
  
  final val CONTROLFILEDELIMITER = "fileDelimiter"
  final val SQOOPEDDATABASE = "sqoopedDatabase"
  
  final val APPNAME = "appName"
  final val REPLACEMENTVALUE = "replacementValue"
  final val COLUMNTOREPLACE = "columnToReplace"
  final val NUMBEROFHEADER = "numOfHeaderLines"
  final val IDCOLUMNS = "colToDeidentify"
  
  final val LOADTYPE = "LoadType"
  final val COUNTINCLUDESHEADER = "CountIncludeHeader"
  final val P1P2CREATEFLAG = "p1p2CreateFlag"
  final val ISSTRINGQUALIFIERDOUBLEQUOTES = "isStringQualifierDoubleQuotes"
}