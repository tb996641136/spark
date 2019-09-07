package wisdom.tb.spark_core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created by BaldKiller
 * on 2019/4/18 10:31
 * 使用HDFS方式创建RDD
 */
public class TBJ4_CreateRDDForHDFS {
    public static void main(String[] args) {
        // 创建SparkConf、SparkContext
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 使用sc.textFile创建本地RDD
        JavaRDD<String> lines = sc.textFile("hdfs://192.168.160.198:9000/hadoop/input_data");

        // 统计一行的长度
        JavaRDD<Integer> mapRDD = lines.map(new Function<String, Integer>() {
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        // 将所有数据进行reduce
        Integer reduceRDD = mapRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 输出
        System.out.println(reduceRDD);
    }
}
