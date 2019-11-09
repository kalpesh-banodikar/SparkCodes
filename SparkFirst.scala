import org.apache.spark.sql.SparkSession

object SparkFirst {


    def main(args: Array[String]) {
      val spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate()
      import spark.implicits._
     val t=spark.read.textFile("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/README.md")
      println(t.count())
      println(t.first())
      val l=t.filter(line=>line.contains("is"))
      println(l.count())
      println(t.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b))
      spark.stop()
  }
}
