package wisdom.tb.spark_core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

/**
 * Created by BaldKiller
 * on 2019/4/18 11:26
 * 统计文本中相同的行数
 */
public class TBJ5_CountLines {
    public static void main(String[] args) {
        // 创建SparkConf、SparkContext
        SparkConf conf = new SparkConf().setAppName("TBJ5_CountLines").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建初始RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\tianbo\\Desktop\\hello.txt");

        // 转换成(lines,1)
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 进行reduceByKey操作
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ":" + stringIntegerTuple2._2);
            }
        });

    }
}
