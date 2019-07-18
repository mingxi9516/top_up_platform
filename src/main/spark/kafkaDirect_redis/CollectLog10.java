package kafkaDirect_redis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * 发送数据（生产者）
 */
public class CollectLog10 {
   public static void main(String[] args){
       //这个是用来配置kafka的参数
       Properties prop = new Properties();
       //这里不是配置broker.id了，这个是配置bootstrap.servers
       prop.put("bootstrap.servers", "192.168.255.111:9092,192.168.255.112:9092,192.168.255.113:9092");
       //下面是分别配置 key和value的序列化
       prop.put("key.serializer",
               "org.apache.kafka.common.serialization.StringSerializer");
       prop.put("value.serializer",
               "org.apache.kafka.common.serialization.StringSerializer");
       //这个地方和1.0X之前的版本有不一样的，这个是使用kafkaproducer 类来实例化
       Producer<String, String> producer = new KafkaProducer<String, String>(prop);
       try {
           BufferedReader bf = new BufferedReader(
                   new FileReader(
                           new File(
                                   "E:\\千锋大数据\\课堂资料\\项目二（充值平台实时统计分析）\\项目（二)（01）\\充值平台实时统计分析\\cmcc.json")));// 路径
           String line = null;
           while((line=bf.readLine())!=null){
               //Thread.sleep(1000);
               producer.send(
                       new ProducerRecord<String, String>(
                               "top_up_platform", line));
           }
           bf.close();
           producer.close();
           System.out.println("已经发送完毕");
       } catch (Exception e) {
           e.printStackTrace();
       }

   }
}
