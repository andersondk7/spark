package org.dka.tutorial.spark.babynames

import scala.language.implicitConversions

/**
  * Represents the [[BabyData]] gender
  *
  * field is either '''M''' for male or '''F''' for female
  */
sealed trait Gender {}
object Gender {
  implicit def fromString(s: String): Gender = s match {
    case "M" => Male
    case "F" => Female
    case _ => Unknown
  }
}
case object Male extends Gender
case object Female extends Gender
case object Unknown extends Gender

