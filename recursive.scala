package com.pradeep.filparser
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{FileSystem, Path}

object mapper {
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val hdfspath="file:///home/cloudera/Desktop/rebate/"

val hdfspath="file:///home/cloudera/Desktop/rebate"
var alldir = ListBuffer[String]()
			
var alldir = ListBuffer[String]()
if(fs.listStatus(new Path(hdfspath)).length == 0) {
println("No Partitions Avaiable, Please check the root dir")
}else
{
fs.listStatus(new Path(hdfspath)).filter(_.isDir).map(_.getPath).foreach(i => BuildListOfFiles(i))
}

def BuildListOfFiles(dir: String):Unit = {
if(fs.listStatus(new Path(dir)).length == 0){alldir += dir+"/*"}
else{
fs.listStatus(new Path(dir)).map(_.getPath).foreach(i => BuildListOfFiles(i.toString))
}}
alldir.foreach(println)

}
