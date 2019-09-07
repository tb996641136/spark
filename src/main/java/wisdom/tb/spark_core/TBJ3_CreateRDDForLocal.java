package wisdom.tb.spark_core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by BaldKiller
 * on 2019/4/18 10:31
 * 使用本地化方式创建RDD
 * 统计文本的字数
 */
public class TBJ3_CreateRDDForLocal {
    public static void main(String[] args) {
        // 创建SparkConf、SparkContext
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // 使用sc.textFile创建本地RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\tianbo\\Desktop\\spark.txt");

        // 先统计每一行的长度 将每一行的数据映射成数据长度
        JavaRDD<Integer> linesLenght = lines.map((str) -> {
            return str.length();
        });

        // 再执行reduce算子 将每一行数据的长度累加起来
        Integer reduceResult = linesLenght.reduce((length1, length2) -> {
            return length1 + length2;
        });

        System.out.println(reduceResult);
    }
}
