package wisdom.tb.spark_core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by BaldKiller 
  * on 2019/4/18 11:16
  * 使用HDFS创建RDD
  */
object TB4_CreateRDDForLocal {
	def main(args: Array[String]): Unit = {
		// 创建SparkConf、SparkContext
		val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)

		val lines: RDD[String] = sc.textFile("hdfs://192.168.160.198:900/hadoop/input_data")
		val lineLenght: RDD[Int] = lines.map(s=>s.length)
		val sum: Int = lineLenght.reduce(_+_)
		println("总数是:"+sum)
	}
}
