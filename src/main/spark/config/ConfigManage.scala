package config

import java.util.Properties

object ConfigManage {
  // 通过类加载器方法来加载指定的配置文件
  private val properties = new Properties()
  try{
    val jdbcConnect = ConfigManage.getClass.getClassLoader.getResourceAsStream("basic.properties")
    properties.load(jdbcConnect)
  }catch {
    case e:Exception =>e.printStackTrace()
  }

  def getProperties(key : String): String ={
    properties.getProperty(key)
  }
}
