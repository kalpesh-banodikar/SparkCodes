import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
case class person(name:String,age:Int)
object CreateDataset extends App{
  val spark= SparkSession.builder().appName("Create Dataset").config("spark.master","local").getOrCreate()
  import spark.implicits._
  val caseDS=Seq(person("Andy",32)).toDS()
  //println(caseDS.show())

  val primDS=Seq(1,2,3).toDS()
  //println(primDS.map(_+1).show())


  var peepDS=spark.read.json("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json").rdd
  println(peepDS.collect().foreach(println))
  val peepDF = spark.sparkContext
    .textFile("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.txt")
    .map(_.split(","))
    .map(attributes => person(attributes(0), attributes(1).toInt))
    .toDF()
  peepDF.createOrReplaceTempView("people")
  val teenDF=spark.sql("SELECT name, age FROM people WHERE age BETWEEN 19 AND 30")
  println(teenDF.map(teen=>"name: "+teen(0)).show())

}
