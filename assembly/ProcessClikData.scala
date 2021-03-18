package com.humira.dataingestion

import scala.util.Try
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.fs.{ FileSystem, Path }
import scala.sys.process._
import org.apache.hadoop.conf.Configuration
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
import com.humira.constants.{ CommonConstants, EnvConstants }
import com.humira.utilities.CustomLogger
import org.apache.spark.sql.SparkSession

object ProcessClikData {
  val logger = new CustomLogger().getLogger(this.getClass.toString())

  def main(arg: Array[String]) {
    def readPropeties(propertyFilePath: String): Properties = {
      new ReadPropeties().readproperty(propertyFilePath)
    }
    var readProp = readPropeties(arg(0))
    val spark = SparkSession.builder().appName(readProp.getProperty(CommonConstants.APPNAME)).getOrCreate()
    //var sparkConf = new SparkConf().setAppName(readProp.getProperty(CommonConstants.APPNAME))
    //var sparkContext = new SparkContext(sparkConf)
    var sparkContext = spark.sparkContext
    val conf = sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val hdfsPath = readProp.getProperty(EnvConstants.LANDINGBASEPATH)
    val hdfsDestPathActivity = readProp.getProperty(EnvConstants.RAWBASEPATHACTIVITY)
    val hdfsDestPathClick = readProp.getProperty(EnvConstants.RAWBASEPATHCLICK)
    val hdfsDestPathImpression = readProp.getProperty(EnvConstants.RAWBASEPATHIMPRESSION)
    val hdfsDestPathLiveStream = readProp.getProperty(EnvConstants.RAWBASEPATHLIVESTREAM)
    val hdfsDestPathReformatted = readProp.getProperty(EnvConstants.RAWBASEPATHREFORMATTED)
    val hdfsDestPathActivityCats = readProp.getProperty(EnvConstants.RAWBASEPATHACTIVITYCATS)
    val hdfsDestPathActivityTypes = readProp.getProperty(EnvConstants.RAWBASEPATHACTIVITYTYPES)
    val hdfsDestPathAdPlacementAssignments = readProp.getProperty(EnvConstants.RAWBASEPATHADPLACEMENTASSIGNMENTS)
    val hdfsDestPathAds = readProp.getProperty(EnvConstants.RAWBASEPATHADS)
    val hdfsDestPathAdvertisers = readProp.getProperty(EnvConstants.RAWBASEPATHADVERTISERS)
    val hdfsDestPathBrowsers = readProp.getProperty(EnvConstants.RAWBASEPATHBROWSERS)
    val hdfsDestPathCampaigns = readProp.getProperty(EnvConstants.RAWBASEPATHCAMPAIGNS)
    val hdfsDestPathCities = readProp.getProperty(EnvConstants.RAWBASEPATHCITIES)
    val hdfsDestPathCreativeAdAssignments = readProp.getProperty(EnvConstants.RAWBASEPATHCREATIVEADASSIGNMENTS)
    val hdfsDestPathCreatives = readProp.getProperty(EnvConstants.RAWBASEPATHCREATIVES)
    val hdfsDestPathCustomCreativeFields = readProp.getProperty(EnvConstants.RAWBASEPATHCUSTOMCREATIVEFIELDS)
    val hdfsDestPathCustomFloodlightVariables = readProp.getProperty(EnvConstants.RAWBASEPATHCUSTOMFLOODLIGHTVARIABLES)
    val hdfsDestPathCustomRichMedia = readProp.getProperty(EnvConstants.RAWBASEPATHCUSTOMRICHMEDIA)
    val hdfsDestPathDesignatedMarketAreas = readProp.getProperty(EnvConstants.RAWBASEPATHDESIGNATEDMARKETAREAS)
    val hdfsDestPathKeywordValue = readProp.getProperty(EnvConstants.RAWBASEPATHKEYWORDVALUE)
    val hdfsDestPathOperatingSystems = readProp.getProperty(EnvConstants.RAWBASEPATHOPERATINGSYSTEMS)
    val hdfsDestPathPaidSearch = readProp.getProperty(EnvConstants.RAWBASEPATHPAIDSEARCH)
    val hdfsDestPathPlacementCost = readProp.getProperty(EnvConstants.RAWBASEPATHPLACEMENTCOST)
    val hdfsDestPathPlacements = readProp.getProperty(EnvConstants.RAWBASEPATHPLACEMENTS)
    val hdfsDestPathSites = readProp.getProperty(EnvConstants.RAWBASEPATHSITES)
    val hdfsDestPathStates = readProp.getProperty(EnvConstants.RAWBASEPATHSTATES)
    
    val fileList = fs.listStatus(new Path(hdfsPath))
    logger.info("Process running for :" + hdfsPath)

    try {
      for (file <- fileList.par) {
        if (!file.isDirectory()) {
          val compressedFilePath = file.getPath.toString()
          val compressedFileName = compressedFilePath.split("\\/").reverse(0)
          val compressedFileNameArray = compressedFileName.split("\\.")
          if (compressedFileNameArray.reverse(0).equalsIgnoreCase("gz")) {
            var hdfsDestPathFinal = ""
            if (compressedFileName.contains("CL_AbbVie_dcm_floodlight4169376_impression")) {
              hdfsDestPathFinal = hdfsDestPathImpression
            } else if (compressedFileName.contains("CL_AbbVie_dcm_floodlight4169376_click")) {
              hdfsDestPathFinal = hdfsDestPathClick
            } else if (compressedFileName.contains("CL_AbbVie_dcm_floodlight4169376_activity")) {
              hdfsDestPathFinal = hdfsDestPathActivity
            }else if (compressedFileName.contains("_reformatted_")) {
              hdfsDestPathFinal = hdfsDestPathReformatted
            }else if (compressedFileName.contains("CL_AbbVie_AbbVie_Humira_site_Clickdata_")) {
              hdfsDestPathFinal = hdfsDestPathLiveStream
            }else if (compressedFileName.contains("_match_table_activity_cats_")) {
              hdfsDestPathFinal = hdfsDestPathActivityCats
            }else if (compressedFileName.contains("_match_table_activity_types_")) {
              hdfsDestPathFinal = hdfsDestPathActivityTypes
            }else if (compressedFileName.contains("_match_table_ad_placement_assignments_")) {
              hdfsDestPathFinal = hdfsDestPathAdPlacementAssignments
            }else if (compressedFileName.contains("_match_table_ads_")) {
              hdfsDestPathFinal = hdfsDestPathAds
            }else if (compressedFileName.contains("_match_table_advertisers_")) {
              hdfsDestPathFinal = hdfsDestPathAdvertisers
            }else if (compressedFileName.contains("_match_table_browsers_")) {
              hdfsDestPathFinal = hdfsDestPathBrowsers
            }else if (compressedFileName.contains("_match_table_campaigns_")) {
              hdfsDestPathFinal = hdfsDestPathCampaigns
            }else if (compressedFileName.contains("_match_table_cities_")) {
              hdfsDestPathFinal = hdfsDestPathCities
            }else if (compressedFileName.contains("_match_table_creative_ad_assignments_")) {
              hdfsDestPathFinal = hdfsDestPathCreativeAdAssignments
            }else if (compressedFileName.contains("_match_table_creatives_")) {
              hdfsDestPathFinal = hdfsDestPathCreatives
            }else if (compressedFileName.contains("_match_table_custom_creative_fields_")) {
              hdfsDestPathFinal = hdfsDestPathCustomCreativeFields
            }else if (compressedFileName.contains("_match_table_custom_floodlight_variables_")) {
              hdfsDestPathFinal = hdfsDestPathCustomFloodlightVariables
            }else if (compressedFileName.contains("_match_table_custom_rich_media_")) {
              hdfsDestPathFinal = hdfsDestPathCustomRichMedia
            }else if (compressedFileName.contains("_match_table_designated_market_areas_")) {
              hdfsDestPathFinal = hdfsDestPathDesignatedMarketAreas
            }else if (compressedFileName.contains("_match_table_keyword_value_")) {
              hdfsDestPathFinal = hdfsDestPathKeywordValue
            }else if (compressedFileName.contains("_match_table_operating_systems_")) {
              hdfsDestPathFinal = hdfsDestPathOperatingSystems
            }else if (compressedFileName.contains("_match_table_paid_search_")) {
              hdfsDestPathFinal = hdfsDestPathPaidSearch
            }else if (compressedFileName.contains("_match_table_placement_cost_")) {
              hdfsDestPathFinal = hdfsDestPathPlacementCost
            }else if (compressedFileName.contains("_match_table_placements_")) {
              hdfsDestPathFinal = hdfsDestPathPlacements
            }else if (compressedFileName.contains("_match_table_sites_")) {
              hdfsDestPathFinal = hdfsDestPathSites
            }else if (compressedFileName.contains("_match_table_states_")) {
              hdfsDestPathFinal = hdfsDestPathStates
            }

            logger.info("Processing " + compressedFileName + "...")
            //val uncompressedFileName = compressedFilePath.split("\\.").dropRight(1).mkString(".")
            val uncompressedFileName = compressedFileNameArray.dropRight(1).mkString(".")
            if (readProp.getProperty(EnvConstants.PARTITIONEDDATA).equalsIgnoreCase("Y")) {
              val FileDataDate = compressedFileNameArray(0).split("\\_").reverse(0)
              val partitionName = readProp.getProperty(EnvConstants.PARTITIONNAME)
              hdfsDestPathFinal = hdfsDestPathFinal + "/" + partitionName + "=" + FileDataDate
            }
            Seq("hdfs", "dfs", "-cat", compressedFilePath) #| Seq("gzip", "-d") #| Seq("hdfs", "dfs", "-put", "-", hdfsDestPathFinal + "/" + uncompressedFileName) !
            //Seq("hdfs", "dfs", "-cat", compressedFilePath) #| Seq("tar", "xfz", "/home/svc-cop-loaddev/TarFiles/fileValues")
            //Seq("hdfs", "dfs", "-put", "/home/svc-cop-loaddev/TarFiles/fileValues/*.tsv", hdfsDestPath)

            fs.delete(new Path(compressedFilePath), true)
          }
          logger.info("Processing Completed for " + compressedFileName + "!!!")
        }
      }
    } catch {
      case e: Exception =>
        {
          logger.error(e.getMessage, e)
          e.printStackTrace()
        }

    }

  }
}