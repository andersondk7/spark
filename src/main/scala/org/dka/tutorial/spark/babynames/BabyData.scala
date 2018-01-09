package org.dka.tutorial.spark.babynames

import scala.language.implicitConversions

/**
  * Represents the line data in the [[BabyNamesFile]]
  * @param year year the data was taken
  * @param name first name of the baby
  * @param county county in which the baby was born
  * @param gender gender of the baby
  * @param count number of babies with the name, gender in the county for the given year
  */
case class BabyData(year: Int, name: Name, county: County, gender: Gender, count: NameCount) { }

object BabyData {
  private val yearIdx = 0
  private val nameIdx = 1
  private val countyIdx: Int = 2
  private val genderIdx = 3
  private val countIdx = 4

  /**
    * constructor
    * @param d array of strings in format: "Year,First Name,County,Sex,Count"
    */
  def apply(d: Array[String]): BabyData =
    BabyData(
      d(yearIdx).toInt,
      d(nameIdx),
      d(countyIdx),
      d(genderIdx),
      d(countIdx).toInt
    )
}
