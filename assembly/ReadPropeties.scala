package com.humira.dataingestion

import java.util.Properties
import java.io.FileInputStream
import com.humira.process.HdfsReader

class ReadPropeties {
  
  def readproperty(propertyFilePath:String) :Properties  ={
    try{
   // val props = new Properties()
     val props = new HdfsReader(propertyFilePath).readAsProperties()
    //props.load(new FileInputStream())
     if (props.isEmpty()) {
       // logger.error("Properties File Empty !!")
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
  
}