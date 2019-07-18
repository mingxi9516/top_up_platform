package kafkaDirect_redis
import com.alibaba.fastjson.{JSON, JSONObject}
import constans.Constans
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object BusinessTwo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(Constans.SET_APP_NAME).master(Constans.SET_MASTER).getOrCreate()
    //读取json文件
    val lines = spark.sparkContext.textFile("file:///E:\\千锋大数据\\课堂资料\\项目二（充值平台实时统计分析）\\项目（二)（01）\\充值平台实时统计分析\\cmcc.json")
    val cityLines = spark.sparkContext.textFile("file:///E:\\千锋大数据\\课堂资料\\项目二（充值平台实时统计分析）\\项目（二)（01）\\充值平台实时统计分析\\city.txt")
    //解析city文件
    val citys = cityLines.map(data => {
      val strings = data.split("\\s")
      (strings(0), strings(1))
    })
    //解析json文件
    val jSONObject: RDD[JSONObject] = lines.map(line => {
      JSON.parseObject(line)
    })
    /**
      * 1。充值请求 sendRechargeReq
      */
    //过滤json数据
    val sendRechargeReq = jSONObject.filter(data=>"sendRechargeReq".equalsIgnoreCase(data.getString("serviceName")))
    //获取字段
    val jsonDate = sendRechargeReq.map(data => {
      val requestId = data.getString("requestId")//业务流水号
      val provinceCode = data.getString("provinceCode")//省份编码
      val startReqTime = data.getString("startReqTime")//请求开始时间
      val endReqTime = data.getString("endReqTime")//请求结束时间
      val bussinessRst = data.getString("bussinessRst")//业务结果
      val successCount = if("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
      val failCount = if("0000".equalsIgnoreCase(bussinessRst)) 0 else 1
      val shouldfee = data.getString("shouldfee").toDouble//订单总金额
      val hour = data.getString("requestId").substring(0, 10)//小时
      val minutes = data.getString("requestId").substring(0, 12) //分钟
      (requestId, provinceCode, startReqTime, endReqTime, bussinessRst,successCount,failCount, shouldfee, hour, minutes)
    })

    /**
      * 以省份为维度统计每个省份的充值失败数,及失败率存入MySQL中
      */
    val failData = jsonDate.map(data => {
      (data._2, (data._7, 1))
    })

    val cityJoin: RDD[(String, ((Int, Int), Option[String]))] = failData.leftOuterJoin(citys)
    val failCountData = cityJoin.map(data => {
      (data._2._2.get, List[Double](data._2._1._1, data._2._1._2))
    })
    val failCountByProvince = failCountData.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })

    val result1 = failCountByProvince.map(data => {
      (data._1, data._2(0), (data._2(0) / data._2(1)).formatted("%.1f"))
    })
    //result1.foreach(println)

  }
}