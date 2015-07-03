package sparkAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;

import scala.Tuple2;

public class mostUsedWeapons {
	
	public static void main( String[] args )
    {
		JavaSparkContext sc = new JavaSparkContext("local", "Java Word Count");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test.events");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        
        mongoRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> arg0)
					throws Exception {
				
				
				return null;
			}});
        
        
        
        
        
        long res =mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("killerweapon");
				System.out.println(arg._2.get("killerweapon"));
				 String str = (String) o;
				          
				return str.equals("Gallil");
			}}).count();
        	
		System.out.println(res);
		
    

    }
}
