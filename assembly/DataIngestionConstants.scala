package com.humira.constants

/**
 * @author CTS Commercial Datalake Team
 *
 */
object DataIngestionConstants  {

	//final val PROPERTIES_FILE_PATH = "/user/svc-cop-loaddev/Resources/Properties/PatientEnrollment/patient_enroll_new.properties"

	final val USER_NAME = "user.name"
			final val APPLICATION_NAME = "Kafka Offset Streaming HBase Ingestion"

			//final val ROW_TAG= "soapenv:Envelope"

			final val SMTP_HOST_KEY = "mail.smtp.host"
			final val SMTP_PORT_KEY = "mail.smtp.port"


			/***********************************************************************PROPERTY FILE KEYS STARTS*****************************************/

			/*Kafka Details*/
			final val KAFKA_BROKER_LIST = "KAFKA_BROKER_LIST"
			final val KAFKA_GROUP_ID = "KAFKA_GROUP_ID"
			final val KAFKA_AUTO_COMMIT_CONFIG = "KAFKA_AUTO_COMMIT_CONFIG"
			final val KAFKA_AUTO_OFFSET_RESET_CONFIG = "KAFKA_AUTO_OFFSET_RESET_CONFIG"
			final val KAFKA_SECURITY_PROTOCOL_CONFIG = "KAFKA_SECURITY_PROTOCOL_CONFIG"
			final val KAFKA_KERBEROS_SERVICE_NAME_KEY = "KAFKA_KERBEROS_SERVICE_NAME_KEY"
			final val KAFKA_KERBEROS_SERVICE_NAME_VALUE = "KAFKA_KERBEROS_SERVICE_NAME_VALUE"
			final val KAFKA_AUTO_COMMIT_INTERVAL_MS_CONFIG = "KAFKA_AUTO_COMMIT_INTERVAL_MS_CONFIG"
			
			final val KAFKA_STREAMING_BATCH_INTERVAL="KAFKA_STREAMING_BATCH_INTERVAL"

			/*Streaming Details*/

			final val STREAMING_CHECKPOINT_DIR = "streamingCheckPointDir"
			final val STREAMING_HDFS_ARCHIVE_DIR = "streamingHDFSArchiveDir"
			final val STREAMING_HDFS_ARCHIVE_JSON_PATH = "streamingHDFSArchiveJSONPath"

			/*ZooKeeper Details*/

			final val HBASE_ZOOKEEPER_QUORUM_KEY = "HBASE_ZOOKEEPER_QUORUM_KEY"
			final val HBASE_ZOOKEEPER_QUORUM_VALUE = "HBASE_ZOOKEEPER_QUORUM_VALUE"
			final val HBASE_ZOOKEEPER_CLIENT_PORT_KEY = "HBASE_ZOOKEEPER_CLIENT_PORT_KEY"
			final val HBASE_ZOOKEEPER_CLIENT_PORT_VALUE= "HBASE_ZOOKEEPER_CLIENT_PORT_VALUE"

			//final val  DATA_LAKE_ID_TEMP_PATH = "/patient_operational_store/datalakeTempPath/TempID"

			final val  DATA_LAKE_ID_TEMP_PATH = "datalakeGenerationTempPath"

			final val DATALAKE_ID= "datalakeSeqNumber"

			final val DATABASE_NAME = "identifiedDatabase"

			final val MASTER_SCHEMA_PATH = "masterSchemaPath"

			/*HBASE*/
			final val HTABLE_PATIENT_DEMOGRAPHICS = "hbasePatientDemographicsTable"
			final val CF_DEMOGRAPHICS = "hbasePatientDemographicsCF"

			final val HTABLE_PATIENT_PROGRAM_DETAILS = "hbasePatientProgramTable"
			final val CF_PROGRAM_DETAILS = "hbasePatientProgramCF"

			final val HTABLE_PATIENT_INTERACTIONS = "hbasePatientInteractionTable"
			final val CF_INTERACTIONS = "hbasePatientInteractionCF"

			final val HTABLE_PATIENT_IDENTIFIER_XREF = "hbaseDatalakeEpsilonXrefTable"
			final val CF_IDENTIFIER_XREF = "hbaseDatalakeEpsilonCF"

			final val HTABLE_EXCEPTION="hbaseExceptionTable"
			final val CF_EXCEPTION="hbaseExceptionCF"

			/*EMAIL NOTIFICATION*/
			final val EMAIL_SENDER_ADDRESS = "emailSenderAddress"
			final val EMAIL_RECEIVER_ADDRESS = "emailReceiverAddress"
			final val EMAIL_SUBJECT = "emailSubject"
			final val EMAIL_CCD_ADDRESS = "emailCCdAddress"
			final val EMAIL_MESSAGE_HEADER = "emailMessageHeader"
			final val EMAIL_MESSAGE_CONTENT = "emailMessageContent"
			final val EMAIL_MESSAGE_FOOTER = "emailMessageFooter"
			final val EMAIL_SMTP_HOST = "emailSMTPHost"
			final val EMAIL_SMTP_PORT = "emailSMTPPort"

			final val EMAIL_TRIGGER_FLAG = "emailTriggerFlag"

			/*COLUMNS*/
			final val COL_CREATED_DATE = "createdDate"
			final val COL_MODIFIED_DATE = "modifiedDate"
			final val COL_CREATED_BY = "createdBy"
			final val COL_MODIFIED_BY = "modifiedBy"
			final val COL_DATA_LAKE_ID = "datalakeID"
			final val COL_IDENTIFIER_ID = "epsilonID"
			final val COL_ACTIVE_FLAG = "activeFlag"

			final val COL_SURROGATE_KEY = "surrogateKey"
			final val COL_PATIENT_PROGRAM = "patientProgram"

			final val PROGRAMNAME = "programName"

			/*CONSTANTS*/
			final val PATIENT = "constantPatient"
			final val PROGRAM = "constantProgram"
			final val INTERACTIONS = "constantInteraction"

			final val HUMIRA = "constantHumira"
			final val ELAGOLIX = "constantElagolix"

			final val PHYSICIAN = "constantPhysician"
			final val AMBASSADOR = "constantAmbassador"

			final val ARRAYOFOTHERINFORMATION = "constantArrayOfOtherInformation"
			final val ARRAYOFTEMPPATIENTIDS = "constantArrayOfTempPatientIds"
			final val ARRAYOFBESTTIMETOCALL = "constantArrayOfBestTimeToCall"

			final val PUBLISHERPATIENTID = "constantPublisherPatientId"
			final val SUBSCRIBERNAME = "constantSubscriberName"
			final val SUBSCRIBERNAME_ALIAS = "constantSubscriberNameAlias"

			final val COL_TYPE_ARRAY = "columnArrayType"
			final val COL_TYPE_STRING = "columnStringType"

			/*CONFIG TABLE*/

			final val COL_HIVE_COL_NAME = "cdloColumnName"
			final val COL_HIVE_COL_POSITION = "cdloColumnPosition"
			final val COL_HIVE_COL_TYPE = "cdloColumnType"
			final val COL_COL_FAMILY = "masterColumnFamily"
			final val COL_HBASE_COL_NAME = "masterColumnName"
			final val COL_TABLE_NAME = "masterTableName"

			final val CONFIGURATION_DATA_PATH = "configurationDataFilePath"
			final val CONFIGURATION_DIR_PATH = "configurationDirPath"

			/*AUDIT TABLE*/
			final val AUDIT_DATABASE = "auditDatabase"
			final val REAL_TIME_STATS_TABLE = "realTimeAuditTable"
			final val REAL_TIME_AUDIT_PATH = "realTimeAuditPath"

			/*STATS TABLE*/
			final val SUCCESS = "msgSuccess"
			final val FAILURE = "msgFailure"

			final val JSON_LOAD_FAILURE = "JSON_LOAD_FAILURE"
			final val MERGE_FAILURE = "MERGE_FAILURE"
			final val TABLE_LOAD_FAILURE = "TABLE_LOAD_FAILURE"

			final val PROFILE_APP_NAME = "PROFILE_APP_NAME"
			final val MANAGE_APP_NAME = "MANAGE_APP_NAME"

			final val IF_VALID_MANAGE_JSON = "IF_VALID_MANAGE_JSON"
			final val IF_VALID_PROFILE_JSON = "IF_VALID_PROFILE_JSON"

			/*MANDATE COLUMNS*/
			final val MANDATORY_COLUMNS = "mandatory_columns"
			final val MANDATORY_FIELDS_MISSING = "MANDATORY_FIELDS_MISSING"

			final val COL_MONITOR_KEY="monitorColumnName"

			/*UPDATE COLUMNS*/

			final val UPDATE_BASED_ON_COLUMNS = "updateBasedOnColumns"
			
			/*KAFKA OFFSET DETAILS*/
			
			final val KAFKA_ZK_QUORUM = "KAFKA_ZK_QUORUM"
			final val KAFKA_ZK_ROOT_DIR = "KAFKA_ZK_ROOT_DIR"
			final val KAFKA_ZK_SESSION_TIMEOUT = "KAFKA_ZK_SESSION_TIMEOUT"
			final val KAFKA_ZK_CONN_TIMEOUT = "KAFKA_ZK_CONN_TIMEOUT"
			final val KAFKA_OFFSET_HTABLE = "KAFKA_OFFSET_HTABLE"
			final val KAFKA_OFFSET_CF = "KAFKA_OFFSET_CF"
			
			
			final val DEIDENTIFIEDXREFTABLE = "deIdentifiedXrefTable"
			final val CONFIGURATIONDATABASENAME = "configurationDataBaseName"
			final val DEIDENTIFIEDAUDITDATABASE = "deidentifiedAuditDatabase"

			/***********************************************************************PROPERTY FILE KEYS ENDS*****************************************/
}