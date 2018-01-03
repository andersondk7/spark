package org.dka.tutorial.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BabyNames {
  val header = "Year,First Name,County,Sex,Count"
  val yearIdx = 0
  val nameIdx = 1
  val countyIdx: Int = 2
  val sexIdx = 3
  val countIdx = 4
}
object SimpleScalaSpark extends App {
  import BabyNames._
  val minPartitions = 3
  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleScalaSpark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val cwd = new java.io.File(".").getCanonicalPath
    val fileName = s"$cwd/babyNames.csv"
    val babyNames = sc
      .textFile(fileName, minPartitions)
      .filter(line => !line.contains(header))
      .map(line => line.split(","))
    val count = babyNames.count
    val countiesCount = babyNames.map(row => row(countyIdx)).distinct.count
//    val countiesCount = babyNames.map(row => row(2)).distinct.count

    println(s"*****************")
    println(s"count: $count")
    println(s"counties: $countiesCount")
    println(s"*****************")
    sc.stop()
  }

}
