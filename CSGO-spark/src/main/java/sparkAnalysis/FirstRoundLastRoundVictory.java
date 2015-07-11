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

public class FirstRoundLastRoundVictory {
	
	public static void main( String[] args )
    {
		JavaSparkContext sc = new JavaSparkContext("local", "Winner of first round wins the match?");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/CSGO.events");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        
        JavaPairRDD<Object, BSONObject> allMatchEndRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("matchEnd");
			}});
        
        JavaPairRDD<String, Integer> mappedPossibilitiesRDD = allMatchEndRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Integer>(){
			@Override
			public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				Object firstRoundWinner = record._2.get("firstroundwinner");
				Object lastRoundWinner = record._2.get("lastroundwinner");
				String frw = (String) firstRoundWinner;
				String lrw = (String) lastRoundWinner;
				if (frw.equals(lrw)){
					return new Tuple2<String, Integer>("# of matches in which team that won first round won also the match", 1);
				} else{
					return new Tuple2<String, Integer>("# of matches in which team that won first round didn't win the match", 1);
				}
			}}); 
        
        JavaPairRDD<String, Integer> countPossibilitiesRDD = mappedPossibilitiesRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
        	public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });
        
        List<Tuple2<String, Integer>> output = countPossibilitiesRDD.collect();
        double totalMatches = 0;
        double frwAndLrwMatches = 0;
        for (Tuple2<String, Integer> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
          totalMatches += tuple._2();
          if (tuple._1().equals("# of matches in which team that won first round won also the match")){
        	  frwAndLrwMatches += tuple._2();
          }
        }
        System.out.println("% of who won first round won also the match: " + (frwAndLrwMatches/totalMatches)*100);
        
        sc.stop();
        sc.close();
    }
}
