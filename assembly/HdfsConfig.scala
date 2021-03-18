package com.humira.process
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
class HdfsConfig {
   /**
   * Method to get the configuration of HDFS
   */
  def getConfig(): Configuration = {
    
   val conf = new Configuration()
   val hdfsCoreSitePath = new Path("/etc/hadoop/conf/core-site.xml")
   val hdfsHDFSSitePath = new Path("/etc/hadoop/conf/hdfs-site.xml")

   conf.addResource(hdfsCoreSitePath)
   conf.addResource(hdfsHDFSSitePath)
   conf.set("fs.hdfs.impl", 
        classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName
    );
    conf.set("fs.file.impl",
        classOf[org.apache.hadoop.fs.LocalFileSystem].getName
    );
    
    return conf;
  }
}






