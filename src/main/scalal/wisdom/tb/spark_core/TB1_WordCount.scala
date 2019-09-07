package wisdom.tb.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BaldKiller 
  * on 2019/4/16 9:43
  */
object TB1_WordCount {
	def main(args: Array[String]): Unit = {
		// 1、创建sparkconf 和 sparkContext
		val conf = new SparkConf().setAppName("ScalaWordcount").setMaster("local")
		val sc = new SparkContext(conf)

		// 2、读取文件中每一行数据
		val lines: RDD[String] = sc.textFile("C:\\Users\\tianbo\\Desktop\\spark.txt",2)

		// 3、将每一行数据按空格进行拆分
		val words: RDD[String] = lines.flatMap(lines => lines.split(" "))

		// 4、将每一行单词映射为(word,1)的形式
		val pairs: RDD[(String, Int)] = words.map(word => (word,1))

		// 5、对每个word进行累加
		val wordCounts: RDD[(String, Int)] = pairs.reduceByKey(_+_)

		// 6、打印输出
		wordCounts.foreach(wordCount => println(wordCount._1+" : "+wordCount._2))
	}

}
