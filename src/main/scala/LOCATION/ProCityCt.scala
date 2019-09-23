package LOCATION
import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProCityCt {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("输入目录不正确")
      sys.exit()
    }
    val Array(inputPath) =args

    val spark = SparkSession
      .builder()
      .appName("ct")
      .master("local")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据--读取生成的parquet文件,它是一个datafream
    val df = spark.read.parquet(inputPath)
    // 注册临时视图---相当与一个表,也就是说数据都在这个表中,然后对表进行操作即可
    df.createTempView("log")
    //用sql进行操作,根据需求对省和市进行分组
    val df2 = spark
      .sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //把所查数据存储到E:下,以省市进行分区

    df2.write.partitionBy("provincename","cityname").json("E:\\procity")

    // 把结果存储到mysql数据库
    // 通过config配置文件依赖进行加载相关的配置信息

    //    val load = ConfigFactory.load()//调用ConfigFactory.load()方法它会加载配置文件信息,进而进行执行

    //    //1. 创建Properties对象
    //    val prop = new Properties()
    //    prop.setProperty("user",load.getString("jdbc.user"))
    //    prop.setProperty("password",load.getString("jdbc.password"))
    //    // 2.存储
    //    df2.write.mode(SaveMode.Append).jdbc(
    //      load.getString("jdbc.url"),load.getString("jdbc.tablName"),prop)

    spark.stop()
  }
}
