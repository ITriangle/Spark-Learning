//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;

//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import scala.Tuple2;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by seentech on 2017/2/20.
 */
public class Application {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {


        //对于所有的 Spark 程序而言，要进行任何操作，首先要创建一个 Spark 的上下文，
        //在创建上下文的过程中，程序会向集群申请资源以及构建相应的运行环境。
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);


        //利用 textFile 接口从文件系统中读入指定的文件，返回一个 RDD 实例对象。
        //RDD 的初始创建都是由 SparkContext 来负责的，将内存中的集合或者外部文件系统作为输入源
        String logFile = "./wordcountdata.txt"; // Should be some file on your system


        JavaRDD<String> lines = ctx.textFile(logFile).cache();

        JavaRDD<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(SPACE.split(s));
                    }
                    /**
                     * In 2.0, FlatMapFunction.call() returns an Iterator rather than Iterable
                     */
//                    @Override
//                    public Iterable<String> call(String s) {
//                        return Arrays.asList(SPACE.split(s)).iterator();
//                    }
                });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();
    }
}
