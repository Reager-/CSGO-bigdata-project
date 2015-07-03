package sparkAnalysis;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SparkMain 
{
	private static final Pattern VIRG = Pattern.compile(",");
    public static void mainmmmm( String[] args )
    {
        String input="/Users/daniel/Desktop/DataGenerator/esempio.txt";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> dati = sc.textFile(input).cache();
        JavaRDD<String> words = dati.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
              return Arrays.asList(VIRG.split(s));
            }
          });
        for(String s:words.collect() ){
        	System.out.println(s);
        }
    
}
}
