package com.humira.driver

import com.humira.constants.{ CommonConstants, DataQualityConstants }
import com.humira.utilities.CustomLogger
import com.humira.datavalidation.DataQualityMgmt
import com.humira.dataingestion.RawPersistence
import com.humira.dataingestion.FileValidation
import com.humira.dataingestion.RawLayer
import com.humira.dataingestion.LandingToRaw
import com.humira.dataingestion.ReadPropeties
import com.humira.dataingestion.RefinedLayer
import com.humira.dataingestion.RenameFiles
import com.humira.datavalidation.DataRestatement

object Driver {
  def main(arg: Array[String]) {

    val logger = new CustomLogger().getLogger(this.getClass.toString())

    if (arg.length > 10) {
      logger.error("Invalid Number of input arguments! " + arg.length)
    }
    if (arg(1) == CommonConstants.DRIVERLAYER1NAME) {
      val layer1obj = new RawLayer()
      layer1obj.main(arg)
    } else if (arg(1).equalsIgnoreCase(DataQualityConstants.DRIVERDATAQUALITY)) {
      DataQualityMgmt.main(arg)
    } else if (arg(0) == CommonConstants.DRIVERCOPYLANDINGTORAW) {
      val LandingToRaw = new LandingToRaw()
      LandingToRaw.main1(arg)
    } else if (arg(1) == CommonConstants.DRIVERLAYERNAME) {
      val layer2obj = new RefinedLayer()
      layer2obj.main(arg)
    } else if (arg(0) == CommonConstants.FILEVALIDATION) {
      val FileValidation = new FileValidation()
      FileValidation.main1(arg)
    } else if (arg(0) == CommonConstants.RENAMEFILE) {
      val RenameFiles = new RenameFiles()
      RenameFiles.main1(arg)
    } else if (arg(1) == CommonConstants.DATARESTATEMENT) {
      DataRestatement.main(arg)
    } else {
      println("Invalid Argument! Please Provide Proper Input")
      sys.exit(1)
    }

  }
}
