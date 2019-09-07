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
 * 将Java开发的WrodCount提交到Spark集群中运行案例
 */
public class TBJ2_WordCountCluster {
    public static void main(String[] args) {

        // 1、如果要提交到集群中运行，就要删除.setMaster属性 让程序自动连接到Spark集群
        SparkConf conf = new SparkConf().
                setAppName("TBJ1_WordCountCluster")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2、文件需要上传到hdfs中读取
        JavaRDD<String> lines = sc.textFile("hdfs://192.168.160.198:8020/root/spark.txt");


        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                System.out.println("执行了一次！");
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceByKey = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + " : " + stringIntegerTuple2._2);
            }
        });

        sc.close();

    }
}
