package wisdom.tb.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BaldKiller 
  * on 2019/4/18 11:31
  */
object TB5_CountLines {
	def main(args: Array[String]): Unit = {
		// 创建SparkConf、SparkContext
		val conf: SparkConf = new SparkConf().setAppName("TB5_CountLines").setMaster("local")
		val sc = new SparkContext(conf)

		// 创建初始RDD
		val lines: RDD[String] = sc.textFile("C:\\Users\\tianbo\\Desktop\\hello.txt")

		// 将lines 转换成(lines,1)的形式
		val pairs: RDD[(String, Int)] = lines.map(lines => (lines,1))

		// 进行reduceByKey操作
		val wordCount: RDD[(String, Int)] = pairs.reduceByKey(_ + _)

		// 打印
		wordCount.foreach(wordCount => println(wordCount._1 + " : " + wordCount._2))
	}
}
