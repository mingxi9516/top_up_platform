package sparkUtils

import constans.Constans
import kafkaDirect_redis.JedisConnectionPool
import org.apache.spark.rdd.RDD

object SaveIntoRedis extends Serializable {
  //获取redis连接
  val jedis = JedisConnectionPool.getConnection()
  def saveIntoRedis(result:RDD[(String, List[Double])]): Unit ={
    val tuples: Array[(String, List[Double])] = result.collect()
     tuples.foreach(t=>{
       jedis.hincrByFloat(t._1,Constans.ALL_COUNT,t._2(0).toLong)
       jedis.hincrByFloat(t._1,Constans.COUNT_MONEY,t._2(1).toLong)
       jedis.hincrByFloat(t._1,Constans.SUCCESS_COUNT,t._2(2).toLong)
       jedis.hincrByFloat(t._1,Constans.COUNT_TIME,t._2(3).toLong)
     })
    //println(jedis.hgetAll("20170412"))
  }

  def saveIntoRedis1(result:RDD[(String, Int)]): Unit ={
    val tuples: Array[(String, Int)] = result.collect()
    tuples.foreach(data=>{
      jedis.hincrByFloat(data._1.substring(0,8)+":",data._1,data._2.toLong)
    })
    //println(jedis.hgetAll("20170412:"))
  }
}
