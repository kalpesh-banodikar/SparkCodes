import org.apache.spark.sql.SparkSession

object app {

  def main(args:Array[String]){
    val spark=SparkSession.builder().appName("Simple Application").config("spark.master","local").getOrCreate()
    val data=spark.read.textFile("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/README.md").cache()
    val As=data.filter(line=>line.contains("a")).count()
    val Bs=data.filter(line=>line.contains("b")).count()
    println(s"no of lines with A: $As, no of lines with B: $Bs")
    spark.stop()
  }
}
