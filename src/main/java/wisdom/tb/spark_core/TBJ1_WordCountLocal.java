package wisdom.tb.spark_core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by BaldKiller
 * on 2019/4/11 14:58
 * 本地运行WrodCount案例
 */
public class TBJ1_WordCountLocal {
    public static void main(String[] args) {
        /*
         *   第一步：就是创建sparkconf 用于配置spark程序的基础配置
         *   .setAppName 设置程序名 便于在浏览器中查看程序运行状况
         *   .setMaster  设置spark程序要连接spark集群的master节点的url 如果设置为local 就代表在本地运行
         * */
        SparkConf conf = new SparkConf().
                setAppName("TBJ1_WordCount")
                .setMaster("local");

        /*
         *   第二步：创建SparkContext对象
         *   SparkContext对象是所有Spark程序的一个入口点
         *   主要作用就是初始化程序的核心组件 比如调度器(DAGSchedule、TaskSchedule),以及向Master节点注册等
         *   但是在不同类型的Spark程序中使用的SparkContext也是不同的
         *   scala 使用 SparkContext
         *   java 使用 JavaSparkContext
         *   以此类推
         * */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
         *   第三步：要针对输入源(hdfs、本地文件)，创建一个初始的RDD
         *   输入源中的数据，会被打散，分配到RDD的每个partition中
         *   RDD中有元素这个概念，如果RDD是hdfs或本地文件，那么每个元素就相当于文件中的一行数据
         * */
        JavaRDD<String> lines = sc.textFile("C:\\Users\\tianbo\\Desktop\\spark.txt");

        /*
         *   第四步：对RDD里面的元素进行transformation操作，也就是一些计算操作
         *   先将每一行 拆分成单个的单词 使用flatMap算子
         *   flatMap算子的作用就是将单个元素 差分成一个或多个元素
         *   flatMap算子内部会传入一个匿名内部类 用于处理
         *   FlatMapFunction 有两个泛型参数 分别代表了输入和输出的类型
         * */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        /*
         *   下一步需要将每一个单词映射为 （单词，1）这种形式
         *   因为这样我们后续才能根据单词作为key，来进行每个单词的累加操作
         *   Java使用的是mapToPair这个算子 将每个元素映射为（v1,v2）这样的Tuple2类型的元素
         *   mapToPair这个算子需要与PairFunction配合使用 ，
         *   PairFunction有三个泛型参数，第一个为输入类型，第二个和第三个为Tuple2的类型
         **/
        JavaPairRDD<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        /*
         *   接着需要对相同key中的value进行累加
         *   使用的是reduceByKey这个算子
         * */
        JavaPairRDD<String, Integer> reduceByKey = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /*
         *   最后使用action操作 触发spark的执行
         *   注意：前面的操作都属于transformation操作，如果spark只有transformation操作程序将不会执行
         *   所以在spark程序当中必须有一个action操作，来触发程序的执行
         *   在这里我们使用foreach操作来遍历RDD中的数据
         * */
        reduceByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + " : " + stringIntegerTuple2._2);
            }
        });


    }
}
