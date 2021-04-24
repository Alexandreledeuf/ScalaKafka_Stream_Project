package model

import play.api.libs.json.Json
import java.time.LocalDate

case class Report(Date:LocalDate,ID: Int,Population: Int,Longitude: Float, Latitude: Float,CitizenName:List[String],CitizenScore:List[Int],Mot:List[String])

object Report {
  implicit val reportformat = Json.writes[Report]

}