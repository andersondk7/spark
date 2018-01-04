package org.dka.tutorial.spark.babynames

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSpec, Matchers}

class BabyNamesSpec extends FunSpec with Matchers {
  private val conf = new SparkConf().setAppName("BabyNamesSpec").setMaster("local[*]")
  private implicit val sc: SparkContext = new SparkContext(conf)

  describe("babyNames") {
    val fileName = BabyNamesFile.fileName() // determine the csv file of data
    val dataRdd = BabyNamesFile.rdd(fileName) // import the file into the RDD
    val numberOfCounties = 124
    val numberOfNames = 36713

    it ("should parse into rows") {
      val count: Long = dataRdd.count()
      count shouldBe 145570
    }
    it ("should define year range") {
      val years:RDD[Year] =
        dataRdd
        .map(_.year) // extract the year only
        .distinct
      val begin = years.min
      val end = years.max
      begin shouldBe 2007
      end shouldBe 2015
    }
    it ("should determine unique counties") {
      val counties: RDD[County] =
        dataRdd
        .map(_.county) // extract the county only
        .distinct()
      val count = counties.count()
      count shouldBe numberOfCounties
    }
    it ("should determine unique names") {
      val names: RDD[Name] =
        dataRdd
          .map(_.name) // extract the name only
          .distinct()
      val count = names.count()
      count shouldBe numberOfNames
    }
    it ("should calculate the number of counties for each name for a specific year") {
      // demo:
      //   filter
      //   map
      //   groupByKey
      //     combines all (key, value) entries into a single (key value) entry
      //     where the new value is the Iterable of all values for the given key
      //   sortBy
      val desiredYear = 2010
      val nameNumberOfCounties: RDD[(County, Int)] =
        dataRdd
          .filter(d => d.year == desiredYear) // only for a given year
          .map(d => (d.name, d.county)) // extract only name and county
          .groupByKey() // group into (name, list of counties)
          .map(e => (e._1, e._2.size)) // convert list of counties for each name to number of counties for the name

      val logan = nameNumberOfCounties.filter(_._1 == "LOGAN").first
      println(s"headName: ${logan._1}")
      println(s"headCountyCount: ${logan._2}")


      // now find the name in the most counties
      val (name, count): (String, Int) =
        nameNumberOfCounties
        .sortBy(_._2, ascending = false)
        .first()

      name shouldBe "JACOB"
      count shouldBe 42
    }
    it("should aggregate names across years") {
      // demo:
      //   map
      //   reduceByKey -
      //       combines all (key, value) entries with the same key into one (key, value) entry
      //       with the new value the result of applying a function to all values of the same key
      //   sortBy (ascending and descending)
      val namesAndCount: RDD[(Name, NameCount)] = dataRdd.map(d => (d.name, d.count)) // (name, count) for all names across all counties, across all years

      // given (n1, c1) and (n1, c2) replace with (n1, c1+c2)
      // repeat until all names have been combined
      // yields (n1, sum(count for all n1's)), (n2, sum(count for all n2's)), etc.)
      val namesCount: RDD[(Name, NameCount)] = namesAndCount.reduceByKey((c1, c2) => c1+c2)

      val mostPopular: (Name, NameCount) = namesCount.sortBy(_._2, ascending = false).first()
      val leastPopular: (Name, NameCount) = namesCount.sortBy(_._2, ascending = true).first()

      mostPopular._1 shouldBe "MICHAEL"
      mostPopular._2 shouldBe 12749

      leastPopular._1 shouldBe "VIANKA"
      leastPopular._2 shouldBe 1
    }
    it("should aggregate counts across counties for a given year") {
      // demo:
      //   map
      //   aggregateByKey -
      //       for each (k, v) converts v into a new value type 'u' i.e. (k, v) => (k, u)
      //       combines all (k, u) entries with the same key into one (key, u) entry
      //       with the new u value the result of applying a function to all u values of the same key
      //     this is similar to reduceByKey except that you have the option to convert the value before combining
      //   sortBy (ascending and descending)
      val desiredYear = 2010
      val countyAndNameCount: RDD[(County, (Name, NameCount))] =
        dataRdd
          .filter(d => d.year == desiredYear) // only for a given year
          .map(d => (d.county, (d.name, d.count))) // (county, (name, count)) for all counties and name combinations in the desired year

      val albany = countyAndNameCount.filter(_._1 == "ALBANY")
      albany.count shouldBe 114 // 114 unique names  in albany in 2010
      val kings = countyAndNameCount.filter(_._1 == "KINGS")
      kings.count shouldBe 708
//      kings.collect().take(5).take(5).foreach(println(_))
      // the births would be the sum of the unique name counts

      // to demonstrate what aggregateByKey does
      //  nameCountToCountOnly is the conversion function, it takes the value (county, count) and returns just the count
      //  sumCounts is the combination function
      // yields (county1, sum of all NameCount for all names) repeated for each county
      val nameCountToCountOnly: (NameCount, (Name, NameCount)) => NameCount = (accumulator: NameCount, tuple: (Name, NameCount)) => {
        accumulator + tuple._2
      }
      val sumCounts: (Int, Int) => Int = (i1: Int, i2: Int) => {
        i1 + i2
      }

      val birthsByCounty: RDD[(County, NameCount)] = countyAndNameCount.aggregateByKey(0)(nameCountToCountOnly, sumCounts)

      val birthsAlbany = birthsByCounty.filter(_._1 == "ALBANY").first()._2
      birthsAlbany shouldBe 1076

      val mostPopular: (County, NameCount) = birthsByCounty.sortBy(_._2, ascending = false).first()
      val leastPopular: (County, NameCount) = birthsByCounty.sortBy(_._2, ascending = true).first()

      println(s"mostPopular: $mostPopular")
      println(s"leastPopular: $leastPopular")

      mostPopular._1 shouldBe "KINGS"
      mostPopular._2 shouldBe 23186
//
      leastPopular._1 shouldBe "SENECA"
      leastPopular._2 shouldBe 10
    }
  }

}
