package org.dka.tutorial.spark.babynames

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSpec, Matchers}

class BabyNamesSpec extends FunSpec with Matchers {
  val conf = new SparkConf().setAppName("BabyNamesSpec").setMaster("local[*]")
  implicit val sc = new SparkContext(conf)

  describe("babyNames") {
    val fileName = BabyNamesFile.fileName() // determine the csv file of data
    val dataRdd = BabyNamesFile.rdd(fileName) // import the file into the RDD

    it ("should parse into rows") {
      val count: Long = dataRdd.count()
      count shouldBe 145570
    }
    it ("should define year range") {
      val years:RDD[Int] =
        dataRdd
        .map(_.year) // extract the year only
        .distinct
      val begin = years.min
      val end = years.max
      begin shouldBe 2007
      end shouldBe 2015
    }
    it ("should define unique counties") {
      val counties: RDD[String] =
        dataRdd
        .map(_.county) // extract the county only
        .distinct()
      val count = counties.count()
      count shouldBe 124
    }
    it ("should calculate the number of counties for each name") {

      val nameCounties: RDD[(String, Iterable[String])] =
        dataRdd
          .map(d => (d.name, d.county)) // extract only name and county
          .groupByKey() // group into (name, list of counties)

      // now find the name in the most counties
      val (name, count): (String, Int) =
        nameCounties
        .map(e => (e._1, e._2.size)) // convert to name, count of counties
        .sortBy(_._2, ascending = false)
        .first()

      name shouldBe "LOGAN"
      count shouldBe 386
    }
  }

}
