import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.functions.col

object SQL_ops extends App{
  val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.master", "local").getOrCreate()

import spark.implicits._

 var df=spark.read.csv("/home/kalpesh/Documents/rdd.csv")
  println(df.collect().foreach(println))
  println(df.select(col("_c0")).show())
    println(df.select(s"_c0"+100).show())
  println(df.filter(col("_c2")>2).collect().foreach(println))
  println(df.groupBy("_c0").count().show())
 df.createOrReplaceTempView("MOVIE")
  val sqldf=spark.sql("SELECT * FROM MOVIE")
  println(sqldf.show())
  var dff2= spark.read.json("/home/kalpesh/Downloads/File/employees.json")
  println(dff2.show())
  var dff3=spark.read.json("/home/kalpesh/Desktop/colors.json").toDF()
  println(dff3.rdd.partitions.size)
  dff3.write.csv("/home/kalpesh/Desktop/df")
  println(dff3.show())

}
