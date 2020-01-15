package com.pradeep.filparser
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.fs.{FileSystem, Path}
import sys.process._

object mapper {
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val hdfspath="file:///home/cloudera/Desktop/rebate/"
val replacestring = "/home/cloudera/Desktop/rebate/;/home/cloudera/Desktop/rebatetmp/"
val fromstring = replacestring.split(";")(0)
val tostring = replacestring.split(";")(1)
var newpath = ""
var oldpath = ""

var updalldir = scala.collection.mutable.Map[String,String]()
updalldir.clear

if(fs.listStatus(new Path(hdfspath)).length == 0) {
println("No Partitions Avaiable, Please check the root dir")
}else
{
fs.listStatus(new Path(hdfspath)).filter(_.isDir).map(_.getPath).foreach(i =>
{if(fs.listStatus(i).length == 0){
println("looping here")
newpath = i.toString().replaceAll(fromstring,tostring)
oldpath = i+"/*"
updalldir += ( oldpath -> newpath)
}
else{fs.listStatus(i).filter(_.isDir).map(_.getPath).foreach(i => 
{
newpath = i.toString().replaceAll(fromstring,tostring)
oldpath = i+"/*"
updalldir += ( oldpath -> newpath)
}
)}})
}

updalldir.foreach(i =>{
println(i._2)
"mkdir -p i._2" !
})



}


