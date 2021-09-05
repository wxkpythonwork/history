
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()

import spark.implicits._

val sc: SparkContext = spark.sparkContext
sc.setLogLevel("WARN")//设置日志输出级别
//todo:3、加载数据
val inputFile = "C:/Users/xk/Downloads/pyspark-loglikelihood-master/input.csv"
val textFile: RDD[String] = sc.textFile(inputFile)

textFile.take(10)

val data = textFile.map(x=>x.split(" "))//.flatMap(a=>a)
val data1 = data.map{x=>{
    val a = x(0).split(",")
    (a(0),a(1))
}}
data1.take(5)

val data2 = sc.parallelize(Array(("a","R2"),("a","R21"),("b","R2"),("b","R21"),("c","16"),("c","12"),("d","4"),("d","3"),("a","291"),
                                ("c","R21"),("c","R2"),("a","R2"),("a","12"),("b","4"),("b","291"),("b","4"),("a","3"),("d","291")))



val mergeData = data1.map{case(uid, item)=>(uid, List(item))}
.reduceByKey(_++_)

val itemPairCnt = mergeData.map{case(uid, itemList)=>{
    val itemPair = itemList.combinations(2).toList.map(p=>(p,1))
    itemPair}}
.flatMap(t=>t)
.map(x=>{(x._1(0),x._1(1),1)})
.toDF("item1","item2","together_cnt")
// .toDF("item1","item2")
// .map(s=>{s.map(a=>List(a._1, a._2).mkString("\t"))})
// .flatMap(a=>a)
// .map(t=>{
//     val row = t.split("\t")
//     (row(1).toList, row(2))
// })
// .reduceByKey(_+_)
// .map(t=>{(t._1, t._2.toInt)})
itemPairCnt.show(100)

val itemClickCnt = data1.map(x=>(x._1,x._2))
.toDF("uid", "item_id")
.groupBy("item_id").count()
.withColumnRenamed("count", "item_count").persist()
itemClickCnt.show()

var mergeDF = itemPairCnt.join(itemClickCnt, itemClickCnt("item_id")===itemPairCnt("item1"), "inner")
.withColumnRenamed("item_count", "item_id1_count")
.drop("item_id")
.join(itemClickCnt, itemClickCnt("item_id")===itemPairCnt("item2"), "inner")
.withColumnRenamed("item_count", "item_id2_count")
.drop("item_id")
.persist()


val userCnt = mergeData.map(t=>t._1).distinct().count()
mergeDF = mergeDF.withColumn("userCnt", lit(userCnt))

mergeDF = mergeDF.withColumn("k12", (mergeDF("item_id2_count") - mergeDF("together_cnt")))
mergeDF = mergeDF.withColumn("k21", (mergeDF("item_id1_count") - mergeDF("together_cnt")))

mergeDF = mergeDF.withColumn("k22", mergeDF("userCnt") - (mergeDF("item_id1_count") + mergeDF("item_id2_count") - mergeDF("together_cnt")))
mergeDF = mergeDF.withColumn("k12", (mergeDF("item_id2_count") - mergeDF("together_cnt")))
mergeDF = mergeDF.withColumnRenamed("together_cnt","k11")


mergeDF.show(100)

import org.apache.spark.sql.types._

// val colNames = List("k11", "k12", "k21", "k22")
// for (colName <- colNames) {
//   mergeDF = mergeDF.withColumn(colName, col(colName).toInt)
// }
mergeDF.show(10)
mergeDF.printSchema

import com.google.common.base.Preconditions
def llratio(k11:Int, k12:Int, k21:Int, k22:Int): Double={
//     Preconditions.checkArgument(k11>0 && k12>0 && k21>0 && k22>0)
    val rowEntry =  entropy(k11+k12, k21+k22)
    val colEntry =  entropy(k11+k12, k21+k22)
    val matrixEntry =  entropy(k11+k12, k21+k22)
    println(rowEntry, colEntry,matrixEntry)
    if ((rowEntry + colEntry - matrixEntry) <0){
        return 0 
    }
        -2.0 * (rowEntry + colEntry - matrixEntry)
} 


def entropy(ele:Int*): Double={
    val sum = ele.sum
    var result = 0.0
    for(x <- ele){
        if (x<0) throw new IllegalArgumentException("eeee")
        val zeroflag = if (x==0) 1 else 0
        result += x * Math.log((x + zeroflag) / sum)
    }
    -result
}

val myUdf = udf(llratio (_:Int, _:Int, _:Int, _:Int): Double)

val mergeDF1 = mergeDF.select($"k11", myUdf($"k11",$"k12",$"k21"，$"k22"))
mergeDF1.show(100)







// import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, LongType}










