package sparkAnalysis;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;

import scala.Tuple2;

public class MapFactionBalance {
	public static void main( String[] args )
    {
		JavaSparkContext sc = new JavaSparkContext("local", "KMeans Killed Positions");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test.events");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        JavaPairRDD<Object, BSONObject> killsRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("roundResults");
			}});
        
        JavaPairRDD<String, Integer> mappedWeaponsRDD = killsRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Integer>(){
			@Override
			public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				Object o = record._2.get("map");
				String mappa = (String) o;
				Object o1 = record._2.get("winningfaction");
				String win = (String) o1;
				Tuple2<String, Integer> coppia;
			    if(win.equals("Terrorist")){
			    	coppia=new Tuple2<String, Integer>(mappa+"T", 1);
			    }else{
			    	coppia=new Tuple2<String, Integer>(mappa+"C", 1);
			    }
			    	
				return coppia;
			}}); 
        JavaPairRDD<String, Integer> countWeaponsUsedRDD = mappedWeaponsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
        	public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });
        List<Tuple2<String, Integer>> output = countWeaponsUsedRDD.sortByKey().collect();
        Iterator<Tuple2<String, Integer>> it=output.iterator();
        while(it.hasNext()) {
        	Tuple2<String, Integer> Counter=it.next();
        	Tuple2<String, Integer> Terrorist=it.next();
        	double tot=Counter._2()+Terrorist._2();
        	Counter._1.substring(0, Counter._1.length()-1);
          System.out.println(Counter._1.substring(0, Counter._1.length()-1) + ": CT = " + (Counter._2()/tot)*100 +", T = "+ (Terrorist._2()/tot)*100);
        }
        
        
        
    }

}
