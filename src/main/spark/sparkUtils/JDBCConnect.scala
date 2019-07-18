package sparkUtils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import config.ConfigManage


object JDBCConnect {
  val user=ConfigManage.getProperties("jdbc.user")
  val password=ConfigManage.getProperties("jdbc.password")
  val driver=ConfigManage.getProperties("jdbc.driver")
  val jdbcUrl = ConfigManage.getProperties("jdbc.url")


  var connection:Connection=null
  def getJDBCConnect(): Connection = {
    try{
      Class.forName(driver)
      connection = DriverManager.getConnection(jdbcUrl,user,password)
    }catch {
      case e:Exception=>e.printStackTrace()
    }
    connection
  }

  def closeAll(conn: Connection, sm: PreparedStatement, rs: ResultSet): Unit = {
    try {
      if (rs != null) {
        rs.close
      }
      if (sm != null) {
        sm.close
      }
      if (conn != null) {
        conn.close
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
