package system

import java.sql._
import scalikejdbc._
import java.time.ZonedDateTime

case class ExpediaRecord(id: Long,
                         date_time: String,
                         site_name: Int,
                         posa_continent: String,
                         user_location_country: Int,
                         user_location_region: Int,
                         user_location_city: Int,
                         orig_destination_distance: Double,
                         user_id: Long,
                         is_mobile: Boolean,
                         is_package: Boolean,
                         channel: Int,
                         srch_ci: String,
                         srch_co: String,
                         srch_adults_cnt: Int,
                         srch_children_cnt: Int,
                         srch_rm_cnt: Int,
                         srch_destination_id: Long,
                         srch_destination_type_id: Int,
                         hotel_id: Long
                        )

object ExpediaRecord extends SQLSyntaxSupport[ExpediaRecord] {
  def apply(rs: WrappedResultSet) = new ExpediaRecord(
    rs.long("id"),
    rs.dateTime("date_time").toString,
    rs.int("site_name"),
    rs.string("posa_continent"),
    rs.int("user_location_country"),
    rs.int("user_location_region"),
    rs.int("user_location_city"),
    rs.doubleOpt("orig_destination_distance").getOrElse(0),
    rs.long("user_id"),
    rs.boolean("is_mobile"),
    rs.boolean("is_package"),
    rs.int("channel"),
    rs.date("srch_ci").toString,
    rs.date("srch_co").toString,
    rs.int("srch_adults_cnt"),
    rs.int("srch_children_cnt"),
    rs.int("srch_rm_cnt"),
    rs.long("srch_destination_id"),
    rs.int("srch_destination_type_id"),
    rs.long("hotel_id"))
}
