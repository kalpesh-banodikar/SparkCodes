import org.apache.spark._
import org.apache.spark.sql.SparkSession

object Rdd {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD Operations").setMaster("local")
    val sc = new SparkContext(conf)

    //parallized collection
    var arr=Array(1,2,3,4,5)
    var distarr=sc.parallelize(arr)
    println(distarr.reduce(_+_))

    //External Dataset
    val text = sc.textFile("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/LICENSE").cache()
    val textlen=text.map(s=>s.length)
    println(textlen.count())
    sc.stop()

  }
}