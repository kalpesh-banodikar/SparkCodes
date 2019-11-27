import org.apache.log4j.Logger
import org.apache.log4j.Logger._
import org.apache.spark.sql.SparkSession
object streamingFirst extends App{
  val a=Logger.getLogger(getClass().getName())
  val spark=SparkSession.builder().appName("Streaming").master("local").getOrCreate()
  import spark.implicits._

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination(20000)

}