//package biaoqian
//
//import com.util._
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
//
//object biaoqian1 {
//  def main(args: Array[String]): Unit = {
//    if (args.length != 4) {
//      println("参数不合法")
//      sys.exit()
//    }
//    //1.接收程序参数    doc代表的是数据字典
//    val Array(inputPath,doc,stopwords,outputPath) = args
//
//    val sparkConf = new SparkConf()
//    sparkConf.setAppName("biaoqian1")
//    sparkConf.setMaster("local[*]")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//
//    val sc: SparkContext = new SparkContext(sparkConf)
//
//    val sQLContext = new SQLContext(sc)
//    //字典文件进行读取
//    val docMap: collection.Map[String, String] = sc.textFile(doc).map(line => {
//      val fields: Array[String] = line.split("\\s", -1)
//      (fields(5), fields(1))
//    }).collectAsMap()
//
//    //对敏感字进行读取
//    val stopwordsMap: collection.Map[String, Int] = sc.textFile(stopwords).map((_,0)).collectAsMap()
//
//    //对上述两者进行广播
//    val docbc: Broadcast[collection.Map[String, String]] = sc.broadcast(docMap)
//    val stopwordsbc: Broadcast[collection.Map[String, Int]] = sc.broadcast(stopwordsMap)
//
//    //读取parque文件--只有SQLContext才能读
//    sQLContext.read.parquet(inputPath).where(Tags.hasSomeUserIdConditition)
//      .map(row=>{
//
//        val ads = Tagsads.makeTags(row)
//        val apps = TagsApp.makeTags(row,docbc.value)
//        val device = TagsDevices.makeTags(row,docbc.value)
//        val keywords = TagsKeywords.makeTags(row,docbc.value)
//
//        ()
//
//      })
//
//    sc.stop()
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
//  }
//}
