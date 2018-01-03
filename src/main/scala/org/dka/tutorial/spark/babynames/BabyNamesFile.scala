package org.dka.tutorial.spark.babynames

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * descriptions of elements in the baby names file
  *
  */
object BabyNamesFile {
  val header = "Year,First Name,County,Sex,Count"

  /**
    * full path file name of data file
    * @param path path to root of project
    */
  def fileName(path: String = new java.io.File(".").getCanonicalPath): String = s"$path/babyNames.csv"

  /**
    * read the file into an [[RDD]] of the [[BabyData]]
    * @param fileName name of csv file with data
    * @param minPartitions number of spark partitions
    * @param sc  [[SparkContext]] into which the [[RDD]] is loaded
    */
  def rdd(fileName: String, minPartitions: Int = 3)(implicit sc: SparkContext): RDD[BabyData] =
    sc
    .textFile(fileName, minPartitions)
    .filter(l => !l.contains(header))
    .map(l => BabyData(l.split(",")))
}