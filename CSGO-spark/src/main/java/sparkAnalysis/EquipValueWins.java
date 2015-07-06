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

public class EquipValueWins {
	public static void main( String[] args )
    {
		JavaSparkContext sc = new JavaSparkContext("local", "EquipValueWins");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test.events");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        JavaPairRDD<Object, BSONObject> roundResultsRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("roundResults");
			}});
        JavaPairRDD<String, Integer> countValueWinsRDD = roundResultsRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Integer>(){
			@Override
			public Tuple2<String, Integer> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				Object o = record._2.get("winningfaction");
				String win = (String) o;
				Object o1 = record._2.get("ctequipvalue");
				Integer ctvalue = (Integer) o1;
				Object o2 = record._2.get("tequipvalue");
				Integer tvalue = (Integer) o2;
				Tuple2<String, Integer> coppia=new Tuple2<String, Integer>("winEqualValue", 1);;
				int res=Math.abs(ctvalue-tvalue);
				if(res>3500){
			    if(win.equals("Terrorist")&&tvalue>=ctvalue){
			    	coppia=new Tuple2<String, Integer>("winHighValue", 1);
			    }else{
			    	coppia=new Tuple2<String, Integer>("winLowValue", 1);
			    }
			    if(win.equals("CounterTerrorist")&&ctvalue>=tvalue){
			    	coppia=new Tuple2<String, Integer>("winHighValue", 1);
			    }else{
			    	coppia=new Tuple2<String, Integer>("winLowValue", 1);
			    }
				}	
				return coppia;
			}}); 
        JavaPairRDD<String, Integer> result = countValueWinsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
        	public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });
    
        List<Tuple2<String, Integer>> output = result.collect();
        for (Tuple2<?,?> tuple : output) {
          System.out.println(tuple._1() + ": " + tuple._2());
        }
    
    
    }

}
