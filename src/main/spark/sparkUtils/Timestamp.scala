package sparkUtils

import java.text.SimpleDateFormat

object Timestamp {
  def getTimestamp(time:String):Long={
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val date = format.parse(time)
    val timestamp: Long = date.getTime
    timestamp
  }
}
