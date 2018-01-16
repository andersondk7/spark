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

//      val logan = nameNumberOfCounties.filter(_._1 == "LOGAN").first
//      println(s"headName: ${logan._1}")
//      println(s"headCountyCount: ${logan._2}")


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
      // the births would be the sum of the unique name counts

      // to demonstrate what aggregateByKey does
      //  nameCountToCountOnly is the aggregation function, it it does 2 things:
      //    extracts the NameCount from the value (county, NameCount) and returns just the NameCount
      //    aggregates the NameCount
      //  sumCounts is the combination function
      // yields sum of all NameCount for all names) repeated for each county
      val nameCountToCountOnly: (NameCount, (Name, NameCount)) => NameCount = (accumulator: NameCount, value: (Name, NameCount)) => {
        accumulator + value._2
      }
      val sumCounts: (Int, Int) => Int = (i1: Int, i2: Int) => {
        i1 + i2
      }

      val birthsByCounty: RDD[(County, NameCount)] = countyAndNameCount.aggregateByKey(0)(nameCountToCountOnly, sumCounts)

      val birthsAlbany = birthsByCounty.filter(_._1 == "ALBANY").first()._2
      birthsAlbany shouldBe 1076

      val mostPopular: (County, NameCount) = birthsByCounty.sortBy(_._2, ascending = false).first()
      val leastPopular: (County, NameCount) = birthsByCounty.sortBy(_._2, ascending = true).first()

      mostPopular._1 shouldBe "KINGS"
      mostPopular._2 shouldBe 23186

      leastPopular._1 shouldBe "SENECA"
      leastPopular._2 shouldBe 10
    }
    it("should determine the county with the most males/females for a given year") {
      type GNC = (Gender, NameCount)
      val desiredYear = 2010
      val county_GNC: RDD[(County, GNC)] =
        dataRdd
          .filter(d => d.year == desiredYear) // only for a given year
          .map(d => (d.county, (d.gender, d.count))) // (county, (gender, count)) for all counties and name combinations in the desired year

//      val albanyUniqueMaleNames =
//        county_GNC
//          .filter(d => d._1 == "ALBANY" && d._2._1 == Male)
//          .count()
//      println(s"\nuniquealbany male names: $albanyUniqueMaleNames")

//      val albanyUniqueFemaleNames =
//        county_GNC
//          .filter(d => d._1 == "ALBANY" && d._2._1 == Female)
//          .count()
//      println(s"\nunique albany female names: $albanyUniqueFemaleNames")

      val nameCountToCountOnly: (NameCount, GNC) => NameCount = (acc: NameCount, value: GNC) => { acc + value._2 }
      val sumCounts: (Int, Int) => Int = (i: Int, j: Int) => {i + j}

      val malesByCounty: RDD[(County, NameCount)] =
        county_GNC
            .filter(d => d._2._1 == Male)
          .aggregateByKey(0)(nameCountToCountOnly, sumCounts)

      val femalesByCounty: RDD[(County, NameCount)] =
        county_GNC
          .filter(d => d._2._1 == Female)
          .aggregateByKey(0)(nameCountToCountOnly, sumCounts)

//      val albanyMaleBirths = malesByCounty.filter(_._1 == "ALBANY").first()._2
//      println(s"\nalbany male births: $albanyMaleBirths")
//      val albanyFemaleBirths = femalesByCounty.filter(_._1 == "ALBANY").first()._2
//      println(s"albany female births: $albanyFemaleBirths")

//      val orleansMaleBirths = malesByCounty.filter(_._1 == "ORLEANS").first()._2
//      println(s"\norleans male births: $orleansMaleBirths")
//      val orleansFemaleBirths = femalesByCounty.filter(_._1 == "ORLEANS").first()._2
//      println(s"orleans female births: $orleansFemaleBirths")

//      val alleganyMaleBirths = malesByCounty.filter(_._1 == "ALLEGANY").first()._2
//      println(s"\nallegany male births: $alleganyMaleBirths")
//      val alleganyFemaleBirths = femalesByCounty.filter(_._1 == "ALLEGANY").first()._2
//      println(s"allegany female births: $alleganyFemaleBirths")

      // if there are more than one county with the most males/females, this will only get the first
      val mostMales: (County, NameCount) = malesByCounty.sortBy(_._2, ascending = false).first()
      val leastMales: (County, NameCount) = malesByCounty.sortBy(_._2, ascending = true).first()

      // if there are more than one county with the least males/females, this will only get the first
      // in this dataset, both ALLEGANY and ORLEANS have 5 females
      val mostFemales: (County, NameCount) = femalesByCounty.sortBy(_._2, ascending = false).first()
      val leastFemales: (County, NameCount) = femalesByCounty.sortBy(_._2, ascending = true).first()

//      println(s"\n")
//      println(s"mostMales: $mostMales")
//      println(s"mostFemales: $mostFemales")

      mostMales._1 shouldBe "KINGS"
      mostFemales._1 shouldBe "KINGS"

//      println(s"leastMales: $leastMales")
//      println(s"leastFemales: $leastFemales")

      leastMales._1 shouldBe "ORLEANS"
      leastFemales._1 shouldBe "ALLEGANY"
    }
    it("should determine the number of males/females in a given year") {
      type GNC = (Gender, NameCount)
      val desiredYear = 2010
      val genderCount: RDD[(Gender, NameCount)] =
        dataRdd
          .filter(d => d.year == desiredYear) // only for a given year
          .map(d => (d.gender, d.count)) // (gender,NameCount)) for all counties and name combinations in the desired year

      // takes 2 (Gender, NameCount) instances and returns a (Gender, NameCount) where the NameCount is the sum of the
      //  input NameCount(s)
      val reductionFunc: (GNC, GNC) => GNC = (d1: GNC, d2: GNC) =>  (d1._1, d1._2 + d2._2)

      val males =
        genderCount
        .filter(_._1 == Male)
        .reduce(reductionFunc)

      val females =
        genderCount
        .filter(_._1 == Female)
        .reduce(reductionFunc)

      males._2 shouldBe 67770
      females._2 shouldBe 48688
    }
    it("should extract a sample of data") {
      //
      // see https://www.ma.utexas.edu/users/parker/sampling/repl.htm for an explanation of sampling with/without
      // replacement.  Basically...

      // sampling conceptually runs through the RDD 'num' times and picks a random element each time so you get
      //   'num' results
      //
      // sampling without replacement
      //
      // means that for each sample run, the element in the previous run is removed from the RDD used in the next run,
      //
      // For example if on the first run element # 47 is picked then on the second run element #47 is not in
      // the RDD.  This means that any given element can be in the results at most once and that the results of the nth
      // sample depends on the (n-1)th sample
      //
      // sampling with replacement
      //
      // means that for each sample run, the entire RDD is used.
      //
      // For example if on the first run element #47 is picked, then on the second run element #47 could also be
      // picked since it is NOT removed from the RDD that is being sampled
      //
      // this becomes a bigger issue for small sample sizes
      val smallRdd = dataRdd.filter(d => d.county == "ALBANY" && d.name == "MICHAEL")
      smallRdd.count() shouldBe 7
//      println(s"smallRdd:\n ${smallRdd.collect().mkString("\n ")}")

      // by choosing the same seed, we get the same sample each time
      val seed = 8
      val sampleSize = 4
      val sampleNoReplacement: Array[BabyData] = smallRdd.takeSample(withReplacement = true, sampleSize, seed)
      val sampleReplacement: Array[BabyData] = smallRdd.takeSample(withReplacement = false,  sampleSize, seed)

//      println(s"no replacement:\n")
//      sampleNoReplacement.foreach(println(_))
//      println(s"\nreplacement:\n")
//      sampleReplacement.foreach(println(_))

      // when withReplacement is false (i.e. sampled elements are removed) the chances of duplicate values increases.
      // in our test, we only have 7 elements and we are going to randomly choose an element 4 times, so the chances of
      // picking the same element are large
      val noReplacementDuplicate = sampleNoReplacement.count(_.year == 2011)
      val replacementDuplicate = sampleReplacement.count(_.year == 2011)

      noReplacementDuplicate shouldBe 2
      replacementDuplicate shouldBe 0
    }
    it("should count the number of males/females for each year") {
      type CNC = (County, NameCount)

      val yearCNCMales: RDD[(Year, CNC)] =
        dataRdd
          .filter(d => d.gender == Male) // only males
          .map(d => (d.year, (d.county, d.count)))
      val yearCNCFemales: RDD[(Year, CNC)] =
        dataRdd
          .filter(d => d.gender == Female)
          .map(d => (d.year, (d.county, d.count)))

      val extractor: (NameCount, CNC) => NameCount = (acc: NameCount, value: CNC) => acc + value._2
      val summer: (NameCount, NameCount) => NameCount = (i: NameCount, j: NameCount) => i + j

      val malesByYear: RDD[(Year, NameCount)] =
        yearCNCMales
          .aggregateByKey(0)(extractor, summer)
          .sortBy(_._1)

      val femalesByYear: RDD[(Year, NameCount)] =
        yearCNCFemales
          .aggregateByKey(0)(extractor, summer)
        .sortBy(_._1)

      println(s"\n******************\nmalesByYear:\n${malesByYear.collect().mkString("\n")}")
      println(s"\n******************\nfemalesByYear:\n${femalesByYear.collect().mkString("\n")}")

    }
    it("should count the number of males and females for each year") {
      type GCN = (Gender, County, NameCount)
      type GenderCount = (Int, Int)

      val yearGCN: RDD[(Year, GCN)] =
        dataRdd
          .map(d => (d.year, (d.gender, d.county, d.count)))

      val extractor: (GenderCount, GCN) => GenderCount = (acc: GenderCount, value: GCN) => {
        val (males, females) = acc  // identify males and female counts
        val nameCount = value._3    // extract nameCount from value
        value match {
          case gcn if gcn._1 == Male => (males + nameCount, females)
          case _ => (males, females + nameCount) // default to female
        }
      }

      val summer: (GenderCount, GenderCount) => GenderCount = (i: GenderCount, j: GenderCount) => (i._1 + j._1 , i._2 + j._2)

      val countsByYear: RDD[(Year, GenderCount)] =
        yearGCN
          .aggregateByKey((0,0))(extractor, summer)
          .sortBy(_._1)

      println(s"\n******************\nByYear:\n${countsByYear.collect().mkString("\n")}\n\n")

    }
  }

}
