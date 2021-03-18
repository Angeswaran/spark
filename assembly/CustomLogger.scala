package com.humira.utilities

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CustomLogger {
  
  def getLogger(loggerClassname:String):Logger ={
    val logger =  LoggerFactory.getLogger(loggerClassname)
     logger
    
  }
}