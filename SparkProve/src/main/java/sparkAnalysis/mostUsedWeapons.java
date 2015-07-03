package sparkAnalysis;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
        
        
        JavaPairRDD<Object, BSONObject> killsRDD =mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("kill");
			}});
        
        JavaPairRDD<String, Integer> coppie=killsRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				    Object o=record._2.get("killerweapon");
				    String nomeArma=(String)o;
				
				return new Tuple2<String, Integer>(nomeArma, 1);
			}});
        
        JavaPairRDD<String, Integer> counts = coppie.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
        }
       sc.stop();
       sc.close();
        
        
        
        
        
   
        	
		
    

    }
}
