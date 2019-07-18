package recharge
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import sparkUtils.{SaveIntoMySql, SaveIntoRedis, Timestamp}

import scala.collection.mutable

object ParseJson {
  //创建一个map用于临时保存计算结果
  var map = new mutable.HashMap[String,(Double,Double)]()
  def parseJsonData(jsonData : RDD[String],ssc: StreamingContext): Unit ={
    /**
      * 充值通知
      * 需求1)	统计全网的充值订单量, 充值金额, 充值成功数,统计充值总时长。
      */
    //解析json
    val parseJson: RDD[JSONObject] = jsonData.map(data => {
      JSON.parseObject(data)
    })
    //过滤数据，取到充值通知的数据
    val filterJson1: RDD[JSONObject] = parseJson.filter(data => {
      "reChargeNotifyReq".equalsIgnoreCase(data.getString("serviceName"))
    })

    //对过吕后的数据设置检查点
    ssc.sparkContext.setCheckpointDir("hdfs://node1:9000//checkpoint//filterJson1")
    filterJson1.checkpoint()
    //获取业务需求字段进行操作
    val baseData1: RDD[(String, List[Double])] = filterJson1.map(data => {
      //获取充值成功的结果数据
      val bussinessRst = data.getString("bussinessRst")
      val money: Double = if ("0000".equalsIgnoreCase(bussinessRst)) data.getDouble("chargefee") else 0.0
      val successCount: Int = if ("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
      val day = data.getString("requestId").substring(0, 8)
      val inTime: Long = Timestamp.getTimestamp(data.getString("requestId").substring(0, 14))
      val outTime: Long = Timestamp.getTimestamp(data.getString("receiveNotifyTime").substring(0, 14))
      (day, List[Double](1, money, successCount, outTime - inTime))
    })
    val result1: RDD[(String, List[Double])] = baseData1.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
   SaveIntoRedis.saveIntoRedis(result1)

    //2)	实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
    val countByMinutes = filterJson1.map(data => {
      //获取充值成功的结果数据
      val bussinessRst = data.getString("bussinessRst")
      val count = if ("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
      val time = data.getString("requestId").substring(0, 12)
      (time,count)
    })
    //按时间聚合value
    val result2: RDD[(String, Int)] = countByMinutes.reduceByKey(_ + _)
    //存入redis
    SaveIntoRedis.saveIntoRedis1(result2)

    /**
      * 2.	业务质量（存入MySQL）
      * 2.1.	全国各省充值业务失败量分布
      * 统计每小时各个省份的充值失败数据量
      */

    val countByHour: RDD[(String, (String, Int))] = filterJson1.map(data => {
      //获取充值成功的结果数据
      val bussinessRst = data.getString("bussinessRst")
      val failureCount: Int = if ("0000".equalsIgnoreCase(bussinessRst)) 0 else 1
      val provinceCode = data.getString("provinceCode")
      val hours = data.getString("requestId").substring(0, 10)
      (provinceCode, (hours, failureCount))
    })

    //读取城市代码文件
    val lines: RDD[String] = ssc.sparkContext.textFile("file:///E:\\千锋大数据\\课堂资料\\项目二（充值平台实时统计分析）\\项目（二)（01）\\充值平台实时统计分析\\city.txt")
    val citys = lines.map(city => {
      (city.split("\\s")(0), city.split("\\s")(1))
    })
    //将citys广播出去
    val broadcastValue = ssc.sparkContext.broadcast(citys.collect())
    //获取广播变量
    val value: Array[(String, String)] = broadcastValue.value
    //将获取的广播变量装成rdd
    val city: RDD[(String, String)] = ssc.sparkContext.parallelize(value)
    //左连接
    val hourProvince: RDD[(String, ((String, Int), Option[String]))] = countByHour.leftOuterJoin(city)
    val hourAndProvince = hourProvince.map(data => {
      ((data._2._1._1, data._2._2.get), data._2._1._2)
    })
    //聚合value
    val countByHourProvince: RDD[((String, String), Int)] = hourAndProvince.reduceByKey(_ + _)
    //存入MySQL
    if(!countByHourProvince.isEmpty()){
      SaveIntoMySql.saveIntoMysql1(countByHourProvince)
    }


    /**
      * 3.	充值订单省份 TOP10（存入MySQL）
      */
     //获取业务需求数据
    val orderSuccess: RDD[(String, (Int, Int))] = filterJson1.map(data => {
      val bussinessRst = data.getString("bussinessRst")
      val successCount: Int = if ("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
      val provinceCode = data.getString("provinceCode")
      (provinceCode, (successCount, 1))
    })
    //通过join通过provincecode取省的名字
   val orderProvince = orderSuccess.leftOuterJoin(city)
    val orderByProvince = orderProvince.map(data => {
      (data._2._2.get, List[Double](data._2._1._1, data._2._1._2))
    })
    //根据省份聚合订单数
    val reduceByProvince = orderByProvince.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })

    val countByProvince: RDD[(String, Double, Double)] = reduceByProvince.map(data => {
      // (data._1, data._2(0), data._2(1),(data._2(0) / data._2(1)).formatted("%.1f"))
      (data._1, data._2(0), data._2(1))
    })
    //将结果存入MySQL
    if(!countByProvince.isEmpty()){
      SaveIntoMySql.saveIntoMysql2(countByProvince)
    }



    /**
      * 实时统计每小时的充值笔数和充值金额
      */
    val countAndMoneyByHour = filterJson1.map(data => {
      val bussinessRst = data.getString("bussinessRst")
      val hour: String = data.getString("requestId").substring(0, 10)
      val successCount: Double = if ("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
      val money: Double = if ("0000".equalsIgnoreCase(bussinessRst)) data.getDouble("chargefee") else 0.0
      (hour, List(successCount, money))
    })
    val reduceCountAndMoney: RDD[(String, List[Double])] = countAndMoneyByHour.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
    val resultCountAndMoney: RDD[(String, Double, Double)] = reduceCountAndMoney.map(data => {
      (data._1, data._2(0), data._2(1))
    })
    if(!resultCountAndMoney.isEmpty()){
      SaveIntoMySql.saveIntoMysql3(resultCountAndMoney)
    }



  }

}
