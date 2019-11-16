import org.apache.spark._
import org.apache.spark.rdd
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDDALLOperations extends App {
 var conf =new SparkConf().setAppName("RDDALLOperations").setMaster("local")
  var sc= new SparkContext(conf)

  var Rdd= sc.textFile("/home/kalpesh/Downloads/spark1/spark-2.4.4-bin-hadoop2.7/LICENSE")
  var Rdd1=Rdd.map(s=>s.length)
  println(Rdd1.reduce((a,b)=>Math.max(a,b)))                        //Map & Reduce
  var Rdd2=sc.parallelize(List(1,2,3))
  println(Rdd2.flatMap(x=>List(x,x,x)).collect().foreach(print))      //flatMap & map
  println(Rdd2.map(x=>List(x,x,x)).collect().foreach(print))
  val data=sc.parallelize(1 to 15)                      //mapPartition  &  mapPartitionWithIndex
  println(data.mapPartitions( x => List(x.next).iterator).collect().foreach(print))
  data.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", "+x).iterator).collect().foreach(println)

  println(data.sample(true,.2).count())            //Sample
  val data2 = sc.parallelize(10 to 20)
  println((data.union(data2)).collect().foreach(println))         //Union
  println((data.intersection(data2)).collect().foreach(println))  //intersection
  println(data.union(data2).distinct().collect().foreach(println)) //Distinct Union
  var Rdd3=Rdd.map(s=>(s,1))
  println((Rdd3.reduceByKey((a,b)=>a+b)).count())                 //reduceByKey
  println(Rdd3.sortByKey().count())                               //sortByKey
  println(Rdd3.groupByKey().collect.foreach(println))               //groupByKey
  val name1 = sc.parallelize(List("abc", "def", "ghi")).map(a => (a, 1))
  val name2 = sc.parallelize(List("ABC", "abc", "GHI")).map(a => (a, 1))
  println((name1.join(name2)).collect.foreach(println))         //join
  println((name1.leftOuterJoin(name2)).collect().foreach(println))  //leftOuterJoin
  println((name1.rightOuterJoin(name2)).collect().foreach(println))  //rightOuterJoin


}
