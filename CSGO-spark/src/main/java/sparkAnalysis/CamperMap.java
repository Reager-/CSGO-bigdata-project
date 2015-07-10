package sparkAnalysis;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class CamperMap {
	public static void main( String[] args )
    {
		JavaSparkContext sc = new JavaSparkContext("local", "CamperMap");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/test.testProblema");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        JavaPairRDD<Object, BSONObject> roundResultsRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("roundResults");
			}});
        
        JavaPairRDD<String, Double> PassiRDD = mongoRDD.mapToPair(new PairFunction<Tuple2<Object, BSONObject>,String,Double>(){
			@Override
			public Tuple2<String, Double> call(Tuple2<Object, BSONObject> record)
					throws Exception {
				Object o = record._2.get("map");
				String mappa = (String) o;
				Object o1 = record._2.get("twalk");
				Double npassi = null;
				if (o1 instanceof String) {
					String p=(String )o1;
					return new Tuple2<String, Double>(mappa.toLowerCase(), 1.0);
				}
				if (o1 instanceof Integer) {
					Integer p = (Integer)o1;
					npassi=p.doubleValue();
				}if (o1 instanceof Double) {
					npassi = (Double)o1;
				}
				return new Tuple2<String, Double>(mappa, npassi);
			}});
        
        JavaPairRDD<String,Iterable<Double>> TotRDD=PassiRDD.groupByKey();
    
        JavaPairRDD<String, String> resultRDD = TotRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Double>>,String,String>(){
			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<Double>> record)
					throws Exception {
				Iterator<Double> it=record._2().iterator();
				
				double tot=0,count=0,max=0,min= Double.MAX_VALUE;
				while(it.hasNext()){
					count++;
					Double passi=it.next();
					if(max<passi.doubleValue() ){
						max=passi;
					}
					if(passi.doubleValue()!=0&&min>passi.doubleValue()){
						min=passi;
					}
					tot=tot+passi;
					
					
				}
				String result=min+"  "+tot/count+"  "+max;
				return new Tuple2<String, String>(record._1(), result);
			}});
        List<Tuple2<String,String>> output = resultRDD.collect();
        for (Tuple2<?,?> tuple : output) {
        	if(!tuple._1.equals(""))
              System.out.println(tuple._1() + ": " + tuple._2());
        }
        
        
        
    
    
    
    }

}
