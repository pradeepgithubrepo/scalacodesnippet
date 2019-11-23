package com.pradeep.filparser
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{FileSystem, Path}

object mapper {
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val hdfspath="file:///home/cloudera/Desktop/rebate/"

val hdfspath="file:///home/cloudera/Desktop/rebate"
var alldir = ListBuffer[String]()
			
if(fs.listStatus(new Path(hdfspath)).length == 0) {
println("No Partitions Avaiable, Please check the root dir")
}else
{
fs.listStatus(new Path(hdfspath)).filter(_.isDir).map(_.getPath).foreach(i =>
{if(fs.listStatus(i).length == 0){alldir += i+"/*"}
else{fs.listStatus(i).filter(_.isDir).map(_.getPath).foreach(i => alldir += i+"/*")}})
}
alldir.foreach(println)

}
