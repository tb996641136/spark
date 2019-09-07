package wisdom.tb.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BaldKiller 
  * on 2019/4/18 11:15
  * 使用本地模式创建RDD
  */
object TB3_CreateRDDForLocal {
	def main(args: Array[String]): Unit = {
		// 创建SparkConf、SparkContext
		val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)

		val lines: RDD[String] = sc.textFile("C:\\Users\\tianbo\\Desktop\\spark.txt")
		val lineLenght: RDD[Int] = lines.map(s=>s.length)
		val sum: Int = lineLenght.reduce(_+_)
		println("总数是:"+sum)
	}
}
