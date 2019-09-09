package wisdom.tb.spark_core;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by BaldKiller
 * on 2019/4/22 15:48
 * Transformation实战操作
 */
public class TBJ5_TransformationOperation {
    public static void main(String[] args) {
        //transformationMap();
        //transformationFilter();
        //transformationFlatMap();
        //transformationGroupByKey();
        //transformationReduceByKey();
        //transformationSortByKey();
        //transformationJoin();
        //transformationCogroup();
        //transformationMapPartition();
        transformationMapPartitionsWithIndex();
    }


    /*
     *   map案例:对分区中的每个元素乘以2
     *   map接收一个匿名内部类Function
     *   Function 第一个参数是传入的类型 第二个参数是返回的类型
     *   在call方法内部对RDD的每一个元素进行处理，然后返回一个新的元素
     * */
    public static void transformationMap() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers);


        JavaRDD<Integer> mapRDD = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                System.out.println("map run....");
                return v1 * 2;
            }
        });

        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }

    /*
     *   mapPartition算子案例：对每个分区中的元素做操作
     *   map与mapPartition的区别
     *      就是map的输入变换函数是应用于RDD中每个元素，而mapPartitions的输入函数是应用于每个分区。
     *      假设一个rdd有10个元素，分成3个分区。如果使用map方法，map中的输入函数会被调用10次；
     *      而使用mapPartitions方法的话，其输入函数会只会被调用3次，每个分区调用1次。
     * */
    public static void transformationMapPartition() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers, 2);

        JavaRDD<Integer> mapPartitionRDD = parallelize.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                System.out.println("mapPartition run ...");
                List<Integer> re = new ArrayList<>();
                while (integerIterator.hasNext()){
                    int a = integerIterator.next();
                    a = a*2;
                    re.add(a);
                }
                return re.iterator();
            }
        });

        mapPartitionRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.print(integer + " ");

            }
        });

        System.out.println();
    }

    /*
    *   mapPartitionsWithIndex算子案例：输出分区索引与内容
    *   mapPartitionsWithIndex与mapPartitions基本相同，只是在处理函数的参数是一个二元元组，
    *   元组的第一个元素是当前处理的分区的index，元组的第二个元素是当前处理的分区元素组成的Iterator
    * */
    public static void transformationMapPartitionsWithIndex() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers, 2);

        JavaRDD<String> mapPartitionsWithIndexRDD = parallelize.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<Integer> integerIterator) throws Exception {
                ArrayList<String> re = new ArrayList<>();
                while (integerIterator.hasNext()) {
                    re.add(Integer.toString(integer) + " : " + integerIterator.toString());
                }
                return re.iterator();
            }
        }, false);

        mapPartitionsWithIndexRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }


    /*
     *   filter算子案例:过滤偶数
     *   filter算子接收一个匿名内部类Function
     *   Function会传入两个参数  第一个参数是RDD的每一个元素 第二个元素为布尔类型
     *   每个元素都会调用call方法 call方法内部会对元素进行一系列逻辑处理 判断这个元素是否是你想要的
     *   如果是就返回ture 如果不是就返回flase
     * */
    public static void transformationFilter() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = sc.parallelize(numbers);

        JavaRDD<Integer> filterRDD = parallelize.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /*
     *   flatMap算子案例：将一行文本 分为多个单词
     *   flatMap算子接收一个匿名内部类FlatMapFunction
     *   FlatMapFunction 有两个参数 第一个是传入的类型 第二个是返回的类型
     *   flatMap算子 会对传入的元素进行逻辑处理，最终产生多个元素
     *   多个元素放入Iterator中进行存储
     *
     * */
    public static void transformationFlatMap() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> numbers = Arrays.asList("hello you", "hello me", "hello word");
        JavaRDD<String> parallelize = sc.parallelize(numbers);

        JavaRDD<String> flatMap = parallelize.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        flatMap.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    /*
     *   groupByKey算子案例：对班级成绩进行分组
     *   groupByKey算子的作用就是将相同key的values值组合到在一起 形成Iterable
     *
     * */
    public static void transformationGroupByKey() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 50),
                new Tuple2<>("class1", 40),
                new Tuple2<>("class2", 55)
        );

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = sc.parallelizePairs(scores);
        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = stringIntegerJavaPairRDD.groupByKey();

        groupByKeyRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println("class : " + stringIterableTuple2._1);
                Iterator<Integer> integers = stringIterableTuple2._2.iterator();
                while (integers.hasNext()) {
                    System.out.print(integers.next() + " ");
                }
                System.out.println();

                System.out.println("=========================");
            }
        });
    }

    /*
     *   reduceByKey算子案例:对班级成绩进行累加
     *   reduceByKey算子的作用就是将相同key的values进行累加
     * */
    public static void transformationReduceByKey() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<>("class1", 90),
                new Tuple2<>("class2", 50),
                new Tuple2<>("class1", 40),
                new Tuple2<>("class2", 55)
        );

        JavaPairRDD<String, Integer> lines = sc.parallelizePairs(scores);
        JavaPairRDD<String, Integer> reduceByKeyRDD = lines.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + " : " + stringIntegerTuple2._2);
            }
        });
    }

    /*
     *   sortByKey算子案例：对成绩进行排序
     *   sortByKey算子的作用就是将每个元素的key进行排序
     *
     * */
    public static void transformationSortByKey() {
        SparkConf conf = new SparkConf().setAppName("transformationMap").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> scores = Arrays.asList(
                new Tuple2<>(90, "tom"),
                new Tuple2<>(60, "tianli"),
                new Tuple2<>(22, "tibo"),
                new Tuple2<>(100, "marry")
        );

        JavaPairRDD<Integer, String> lines = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, String> sortByKey = lines.sortByKey(false);
        sortByKey.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._2 + " : " + integerStringTuple2._1);
            }
        });
    }

    /*
     *   join算子案例：打印每个学生的成绩
     *   join作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
     * */
    public static void transformationJoin() {
        SparkConf conf = new SparkConf().setAppName("transformationJoin").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> studentName = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(1, "jon"),
                new Tuple2<>(1, "hahahah"),
                new Tuple2<>(2, "tianli"),
                new Tuple2<>(3, "tibo"),
                new Tuple2<>(4, "marry")
        );

        List<Tuple2<Integer, Integer>> studentScore = Arrays.asList(
                new Tuple2<>(1, 90),
                new Tuple2<>(1, 222),
                new Tuple2<>(2, 80),
                new Tuple2<>(3, 60),
                new Tuple2<>(4, 100)
        );

        // 并行化两个集合
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(studentName);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(studentScore);

        // 执行join操作
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nameRDD.join(scoreRDD);

        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.printf("id: %d\nname : %s\nscore : %d\n",
                        integerTuple2Tuple2._1, integerTuple2Tuple2._2._1, integerTuple2Tuple2._2._2);
                System.out.println("============================================");

            }
        });


    }

    /*
     *   cogroup算子
     *   cogroup算子作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
     *   也就是将相同Key的数据聚合在一个迭代器里面
     * */
    public static void transformationCogroup() {
        SparkConf conf = new SparkConf().setAppName("transformationCogroup").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> studentName = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(1, "jon"),
                new Tuple2<>(1, "hahahah"),
                new Tuple2<>(2, "tianli"),
                new Tuple2<>(3, "tibo"),
                new Tuple2<>(4, "marry")
        );

        List<Tuple2<Integer, Integer>> studentScore = Arrays.asList(
                new Tuple2<>(1, 90),
                new Tuple2<>(1, 222),
                new Tuple2<>(2, 80),
                new Tuple2<>(3, 60),
                new Tuple2<>(4, 100)
        );
        // 并行化两个集合
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(studentName);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(studentScore);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroupRDD = nameRDD.cogroup(scoreRDD);
        cogroupRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2) throws Exception {
                System.out.println("id : " + integerTuple2Tuple2._1);
                for (String s : integerTuple2Tuple2._2._1) {
                    System.out.print(s + " ");
                }
                System.out.println();
                for (Integer integer : integerTuple2Tuple2._2._2) {
                    System.out.print(integer + " ");
                }
                System.out.println();
                System.out.println("=============================");
            }
        });


    }


}
