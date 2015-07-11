package sparkAnalysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.linalg.Vector;
import org.bson.BSONObject;

import scala.Tuple2;

public class WinningPatternsEquipment {
	public static void main( String[] args )
    {
		double minSupport= Double.parseDouble(args[0]);
		int numPartitions=Integer.parseInt(args[1]);
		
		JavaSparkContext sc = new JavaSparkContext("local", "Winning Patterns Equipment");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/CSGO.events");
        
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        
        JavaPairRDD<Object, BSONObject> roundResultsRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("roundResults");
			}});
        
        JavaRDD<ArrayList<String>> equipmentRDD = roundResultsRDD.map(new Function<Tuple2<Object, BSONObject>, ArrayList<String>>(){
			@Override
			public ArrayList<String> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				Object winningFaction = record._2.get("winningfaction");
				String winner = (String) winningFaction;
				if (winner.equals("CounterTerrorist")){
					Object ctEquipment = record._2.get("ctequipment");
					String strCTEquipment = (String) ctEquipment;
					String[] strArray = strCTEquipment.split("END");
					for(String str:strArray){
						str = str.replaceAll("Knife", "");
						str = str.replaceAll("Bomb", "");
						ArrayList<String> result = Lists.newArrayList(str.split("-"));
						result.removeAll(Arrays.asList(null, ""));
						return result;
					}
					} else{
					Object tEquipment = record._2.get("tequipment");
					String strTEquipment = (String) tEquipment;
					String[] strArray = strTEquipment.split("END");
					for(String str:strArray){
						str = str.replaceAll("Knife", "");
						str = str.replaceAll("Bomb", "");
						ArrayList<String> result = Lists.newArrayList(str.split("-"));
						result.removeAll(Arrays.asList(null, ""));
						return result;
					}
					}
				return null;
			}}); 
        
        FPGrowthModel<String> model = new FPGrowth()
        .setMinSupport(minSupport)
        .setNumPartitions(numPartitions)
        .run(equipmentRDD);
        
        LinkedList<Tuple2<String, Long>> collection = new LinkedList<Tuple2<String, Long>>();
        
		for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			String pattern = "[" + Joiner.on(",").join(itemset.javaItems()) + "]: ";
			long freq = itemset.freq();
			collection.add(new Tuple2<String, Long>(pattern,freq));
		}
		
		JavaRDD<Tuple2<String, Long>> collectionRDD = sc.parallelize(collection);
		JavaPairRDD<String, Long> collectionPairRDD = JavaPairRDD.fromJavaRDD(collectionRDD);
		
		JavaPairRDD<Long, String> swappedPairRDD = collectionPairRDD.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Long> item) throws Exception {
                return item.swap();
            }

         });
        		
        JavaPairRDD<Long, String> swappedOrderedPatternsRDD = swappedPairRDD.sortByKey(false);
        
        JavaPairRDD<String, Long> resultRDD = swappedOrderedPatternsRDD.mapToPair(new PairFunction<Tuple2<Long, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<Long, String> item) throws Exception {
                return item.swap();
            }

         });
        
        List<Tuple2<String, Long>> output = resultRDD.collect();
        for (Tuple2<String,Long> tuple : output) {
          System.out.println(tuple._1() + " " + tuple._2());
        }
    }
}
