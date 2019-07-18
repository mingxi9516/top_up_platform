package sparkUtils


import java.sql.{PreparedStatement, SQLException}

import org.apache.spark.rdd.RDD



object SaveIntoMySql {
  /**
    * 统计每小时各个省份的充值失败数据量
    */
  def saveIntoMysql1(result1:RDD[((String, String), Int)]): Unit = {
    result1.foreachPartition(data => {
      data.foreach(t => {
        val connect = JDBCConnect.getJDBCConnect()
        val sql = "insert into countbyhourandprovince(hour,province,count) values(?,?,?)"
        var statement: PreparedStatement = null
        try {
          statement = connect.prepareStatement(sql)
          statement.setString(1,t._1._1)
          statement.setString(2,t._1._2)
          statement.setInt(3,t._2)
          val i = statement.executeUpdate()
          if (i > 0) {
            println("插入成功！！！")
          }
        } catch {
          case e: SQLException => e.printStackTrace()
        } finally {
          JDBCConnect.closeAll(connect, statement, null)
        }
      })
    })
  }
  /**
    * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，
    * 只保留一位小数，存入MySQL中，进行前台页面显示。
    */
  def saveIntoMysql2(result2:RDD[(String, Double, Double)]): Unit ={
    result2.foreachPartition(data=>{
      data.foreach(t=>{
        val connect = JDBCConnect.getJDBCConnect()
        val sql = "insert into countbyprovinceTop10(province,successCount,count) values(?,?,?)"
        var statement:PreparedStatement = null
          try{
            statement = connect.prepareStatement(sql)
            statement.setString(1,t._1)
            statement.setDouble(2,t._2)
            statement.setDouble(3,t._3)
          val i = statement.executeUpdate()
          if(i > 0){
            println("插入成功！！！")
          }
        }catch {
          case e:SQLException=>e.printStackTrace()
        }finally {
          JDBCConnect.closeAll(connect,statement,null)
        }
      })
    })
  }

  /**
    * 实时统计每小时的充值笔数和充值金额
    */
  def saveIntoMysql3(result3:RDD[(String, Double, Double)]): Unit ={
    result3.foreachPartition(data=>{
      data.foreach(t=>{
        val connect = JDBCConnect.getJDBCConnect()
        val sql = "insert into countAndMoneyByHour(hour,count,money) values(?,?,?)"
        var statement:PreparedStatement = null
        try{
          statement = connect.prepareStatement(sql)
          statement.setString(1,t._1)
          statement.setDouble(2,t._2)
          statement.setDouble(3,t._3)
          val i = statement.executeUpdate()
          if(i > 0){
            println("插入成功！！！")
          }
        }catch {
          case e:SQLException=>e.printStackTrace()
        }finally {
          JDBCConnect.closeAll(connect,statement,null)
        }
      })
    })
  }
}
