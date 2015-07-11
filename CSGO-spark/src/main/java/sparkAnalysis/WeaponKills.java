package sparkAnalysis;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoOutputFormat;

import scala.Tuple2;

public class WeaponKills {
	
	public static void main( String[] args )
    {
		JavaSparkContext sc = new JavaSparkContext("local", "Most Used Weapons");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/CSGO.events");
        /*scrivere su Mongo
        config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/CSGO.outputWeaponKills");

        */
        
        
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        
        JavaPairRDD<Object, BSONObject> killsRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("kill");
			}});
        
        JavaPairRDD<String, Integer> mappedWeaponsRDD = killsRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Integer>(){
			@Override
			public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				Object weapon = record._2.get("killerweapon");
				String str = (String) weapon;
				return new Tuple2<String, Integer>(str, 1);
			}}); 
        
        JavaPairRDD<String, Integer> countWeaponsUsedRDD = mappedWeaponsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
        	public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });
        
        JavaPairRDD<Integer, String> swappedPairRDD = countWeaponsUsedRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

         });
        		
        JavaPairRDD<Integer, String> swappedOrderedCountWeaponsUsedRDD = swappedPairRDD.sortByKey(false);
        
        JavaPairRDD<String, Integer> orderedCountWeaponsUsedRDD = swappedOrderedCountWeaponsUsedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }

         });
        
        List<Tuple2<String, Integer>> output = orderedCountWeaponsUsedRDD.collect();
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
        }
        
        /*codice scrive su MongoDB
        JavaPairRDD<Object, BSONObject> save = orderedCountWeaponsUsedRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Object, BSONObject>() {
        	@Override
        	public Tuple2<Object, BSONObject> call(Tuple2<String, Integer> tuple) {
        	BSONObject bson = new BasicBSONObject();
        	                bson.put("Weapon", tuple._1);
        	                bson.put("NumberOfKills", tuple._2);
        	return new Tuple2<>(null, bson);
        	            }
        	        });
        
        save.saveAsNewAPIHadoopFile("file:///bogus", Object.class, Object.class, MongoOutputFormat.class, config);
        
        */
        
        
        
        sc.stop();
        sc.close();
    }
}
