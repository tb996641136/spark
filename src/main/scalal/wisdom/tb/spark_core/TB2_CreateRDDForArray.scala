package wisdom.tb.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BaldKiller 
  * on 2019/4/18 11:15
  * 使用数组方式创建RDD
  */
object TB2_CreateRDDForArray {
	def main(args: Array[String]): Unit = {
		// 1、创建SparkConf
		val conf: SparkConf = new SparkConf().setAppName("TB2_CreateRDDForArray").setMaster("local")

		// 2、创建SparkContext
		val sc = new SparkContext(conf)

		// 3、创建并行化集合 parallelize第二个参数是想要创建几个partition
		val numbers = Array(1,2,3,4,5,6,7,8,9,10)
		val numbersRDD: RDD[Int] = sc.parallelize(numbers,5)

		// 4、执行reduce操作
		val reduceResult: Int = numbersRDD.reduce(_+_)

		println(reduceResult)
	}
}
