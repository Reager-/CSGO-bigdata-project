package SparkProve.SparkProve;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;

import scala.Tuple2;

public class SparkMongoDB {
	
	public static void main( String[] args )
    {
JavaSparkContext sc = new JavaSparkContext("local", "Java Word Count");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test.events");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        /*JavaRDD<String> words = mongoRDD.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, String>() {
            public Iterable<String> call(Tuple2<Object, BSONObject> arg) {
                Object o = arg._2.get("qty");
                if (o instanceof String) {
                    String str = (String) o;
                    str = str.toLowerCase().replaceAll("[.,!?\n]", " ");
                    return Arrays.asList(str.split(" "));
                } 
            }
        });*/
        long res =mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("killerweapon");
				System.out.println(arg._2.get("killerweapon"));
				 String str = (String) o;
				          
				return str.equals("AK47");
			}}).count();
        	
		System.out.println(res);
		
    

    }
}
