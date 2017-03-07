import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.regex.Pattern;


/**
 * Created by seentech on 2017/2/20.
 */
public class Application {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        String logFile = "./wordcountdata.txt"; // Should be some file on your system
        SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(logFile).cache();

        long numAs = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        ctx.stop();


    }
}
