package com.humira.process


import java.util.Properties
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
 *  
 */
case class HdfsReader(path: String) extends HdfsConfig{

  def getFS(): FileSystem = {
    //val conf = new Configuration()
    FileSystem.get(getConfig())
  }

  def readAsProperties(): Properties = {
    val properties = new Properties()
    val pt = new Path(path)
    val fs = getFS()
    val stream = fs.open(pt)
    properties.load(stream)
    properties
  }

  def readAsLines(): Iterator[String] = {
    val pt = new Path(path)
    val fs = getFS()
    val stream = fs.open(pt)
    val result = Source.fromInputStream(stream).getLines()
    result
  }
}