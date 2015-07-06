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
		JavaSparkContext sc = new JavaSparkContext("local", "Most Used Weapons");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test.events");
        /*scrivere su Mongo
        config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/test.outputWeaponKill");

        */
        
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
				String str = (String) winningFaction;
				if (str.equals("CounterTerrorist")){
					Object ctEquipment = record._2.get("ctequipment");
					String strCTEquipment = (String) ctEquipment;
					String[] arrayStr = strCTEquipment.split("-");
					return Lists.newArrayList(strCTEquipment.split("-"));
				} else{
					Object tEquipment = record._2.get("tequipment");
					String strTEquipment = (String) tEquipment;
					return Lists.newArrayList(strTEquipment.split("-"));
				}
			}}); 
        	
        equipmentRDD.cache();
			FPGrowth fpg = new FPGrowth()
			  .setMinSupport(0.3)
			  .setNumPartitions(1);
			FPGrowthModel<String> model = fpg.run(equipmentRDD);

			for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			   System.out.println("[" + Joiner.on(",").join(itemset.javaItems()) + "], " + itemset.freq());
			}
    }
}
