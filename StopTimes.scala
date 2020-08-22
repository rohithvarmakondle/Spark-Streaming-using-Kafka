package ca.rohith.bigdata.applications

case class StopTimes(
                      trip_id: String,
                      arrival_time: String,
                      departure_time: String,
                      stop_id: Int,
                      stop_sequence: Int
                    )

object StopTimes {
  def apply(csvline: String): StopTimes = {
    val a = csvline.split(",", -1)
    StopTimes(a(0), a(1), a(2), a(3).toInt, a(4).toInt)
  }
}