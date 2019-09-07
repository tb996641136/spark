package wisdom.tb.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BaldKiller 
  * on 2019/4/22 16:41
  * scala版本 transformation操作
  */
object TB5_TransformationOperation {
	def main(args: Array[String]): Unit = {
		//map()
		//filter()
		//flatMap()
		//groupByKey()
		//reduceByKey()
		//sortByKey()
		//join()
		cogroup()

	}

	/*
	* 	map算子案例：对每个元素乘以2
	* */
	def map(): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)

		val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		val lines: RDD[Int] = sc.parallelize(numbers, 2)
		val mapToPaires: RDD[Int] = lines.map(line => line * 2)
		mapToPaires.foreach(n => println(n))

	}

	/*
	* 	filter算子案例：过滤偶数
	* */
	def filter(): Unit = {
		val conf: SparkConf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

		val lines: RDD[Int] = sc.parallelize(numbers, 2)
		val filterRDD: RDD[Int] = lines.filter(number => number % 2 == 0)
		filterRDD.foreach(x => println(x))
	}

	/*
	* 	flatMap算子案例：将字符串拆分为单个单词
	* */
	def flatMap(): Unit = {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val arr = Array("hello word", "hello me", "hello you")

		val lines: RDD[String] = sc.parallelize(arr, 2)
		val word: RDD[String] = lines.flatMap(line => line.split(" "))
		word.foreach(w => println(w))
	}

	/*
	* 	groupByKey算子案例：对班级成绩进行分组
	* */
	def groupByKey(): Unit = {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val scrores = Array(Tuple2("class1", 50), Tuple2("class2", 80), Tuple2("class1", 80), Tuple2("class2", 55))

		val lines: RDD[(String, Int)] = sc.parallelize(scrores)
		val groupByKeyRDD: RDD[(String, Iterable[Int])] = lines.groupByKey()
		groupByKeyRDD.foreach(
			x => {
				println("class : " + x._1)
				x._2.foreach(v => print(v + " "))
				println()
				println("=================")
			}
		)
	}

	/*
	* 	reduceByKey算子案例：将班级成绩累加
	*
	* */
	def reduceByKey(): Unit = {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val scrores = Array(Tuple2("class1", 50), Tuple2("class2", 80), Tuple2("class1", 80), Tuple2("class2", 55))

		val lines: RDD[(String, Int)] = sc.parallelize(scrores)
		val reduceByKeyRDD: RDD[(String, Int)] = lines.reduceByKey(_ + _)
		reduceByKeyRDD.foreach(x => println(x._1 + " : " + x._2))
	}

	/*
	* 	sortByKey算子案例：对成绩进行排序
	* */
	def sortByKey(): Unit = {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val scrores = Array(Tuple2(50, "Tl"), Tuple2(80, "tom"), Tuple2(99, "marry"), Tuple2(55, "Tina"))

		val lines: RDD[(Int, String)] = sc.parallelize(scrores)
		val sortByKeyRDD: RDD[(Int, String)] = lines.sortByKey(false)
		sortByKeyRDD.foreach(x => println(x._2 + " : " + x._1))
	}

	/*
	* 	join算子案例:打印每个学生的成绩
	* */
	def join(): Unit = {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val studentName = Array(Tuple2(1, "Tl"), Tuple2(2, "tom"), Tuple2(3, "marry"), Tuple2(4, "Tina"))
		val studentScores = Array(Tuple2(1, 100), Tuple2(2, 125), Tuple2(3, 68), Tuple2(4, 23))

		val scoreRDD: RDD[(Int, Int)] = sc.parallelize(studentScores)
		val nameRDD: RDD[(Int, String)] = sc.parallelize(studentName)

		val joinRDD: RDD[(Int, (String, Int))] = nameRDD.join(scoreRDD)

		joinRDD.foreach(x => {
			println("id : " + x._1)
			println("name : " + x._2._1)
			println("score : " + x._2._2)
			println("======================")
		})
	}

	def cogroup(): Unit = {
		val conf = new SparkConf().setAppName("map").setMaster("local")
		val sc = new SparkContext(conf)
		val studentName = Array(Tuple2(1, "Tl"), Tuple2(2, "tom"), Tuple2(3, "marry"), Tuple2(4, "Tina"),Tuple2(1, "wwwww"),Tuple2(1, "fuck"))
		val studentScores = Array(Tuple2(1, 100), Tuple2(2, 125), Tuple2(3, 68), Tuple2(4, 23))

		val scoreRDD: RDD[(Int, Int)] = sc.parallelize(studentScores)
		val nameRDD: RDD[(Int, String)] = sc.parallelize(studentName)

		val cogroupRDD: RDD[(Int, (Iterable[String], Iterable[Int]))] = nameRDD.cogroup(scoreRDD)

		cogroupRDD.foreach(x =>{
			println("id : " + x._1)
			x._2._1.foreach(xx => println(xx))
			x._2._2.foreach(yy => println(yy))
			println("====================")
		})
	}
}
