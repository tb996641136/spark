package wisdom.tb.spark_core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by BaldKiller
 * on 2019/4/18 10:30
 * 使用数组创建RDD案例
 */
public class TBJ3_CreateRDDForArray {
    public static void main(String[] args) {
        // 1、创建SparkConf
        SparkConf conf = new SparkConf().setAppName("TBJ3_CreateRDDForArray").setMaster("local");

        // 2、创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3、使用JavaSparkContext的parallelize()方法构造并行化集合
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        /*
        *   4、进行reduce算子操作
        *   reduce算子就是将集合中的值依次相加
        *   比如：1+2 = 3 使用1+2的结果3与后面的数相加 及 3+3 = 6   6+4 = 10    10+5 = 15 .....以此类推
        * */
        Integer reduceResult = numbersRDD.reduce((v1, v2) -> {
            return v1 + v2;
        });

        System.out.println(reduceResult);


    }
}
