import org.apache.spark.sql.SparkSession

object Wordcount{
  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Simple Application").config("spark.master", "local").getOrCreate()

    import spark.implicits._

    val t = spark.read.textFile("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/README.md")
    val words = t.flatMap(line => line.split(" ")).groupByKey(identity).count
    println(words.count())
    println(words.collect())

    spark.stop()

  }
}