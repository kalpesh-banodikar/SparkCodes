import org.apache.spark._
object RDD_Operations extends App {

  var conf=new SparkConf().setAppName("RDD Application").setMaster("local")
  var sc=new SparkContext(conf)
  var file=sc.textFile("/home/kalpesh/Documents/rdd.csv")
  println(file.collect().foreach(println))
  var file1=file.map(_.split(",")(2))
  println(file1.collect().foreach(println))
  val scount=file1.map(scount=>(scount,1))
  println(scount.collect().foreach(println))
  val add=scount.reduceByKey((x,y)=>x+y).map(tup=>(tup._2,tup._1))sortByKey (true)
  println(add.collect().foreach(println))
  val flat=file.flatMap(line=>line.split("family"))
  println(flat.collect().foreach(println))
  val fil=file.filter(line=>line.contains("funny"))
  println(fil.collect().foreach(println))
  val fil2=file.filter(line=>line.contains("family"))
  var uni=fil.union(fil2)
  println(uni.collect().foreach(println))



}
