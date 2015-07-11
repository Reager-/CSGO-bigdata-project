package sparkAnalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.bson.BSONObject;

import scala.Tuple2;

public class KillsHeatZones {
	
	public static void main( String[] args )
    {
		String map=args[0];
		JavaSparkContext sc = new JavaSparkContext("local", "KMeans Killed Positions");
        
        Configuration config = new Configuration();
        config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/CSGO.events");
        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);
        
        JavaPairRDD<Object, BSONObject> killedPositionsRDD = mongoRDD.filter(new Function<Tuple2<Object, BSONObject>, Boolean>() {
        	public Boolean call(Tuple2<Object, BSONObject> arg)throws Exception {
				Object o = arg._2.get("event");
				 String str = (String) o;
				 return str.equals("kill");
			}});
        
        JavaRDD<Vector> vectorsRDD = killedPositionsRDD.map(new Function<Tuple2<Object, BSONObject>, Vector>() {
        	        public Vector call(Tuple2<Object, BSONObject> record) throws Exception {
        	        Object mapname = record._2.get("map");
            	    String scheck = (String) mapname;
            	    scheck=scheck.toLowerCase();
            	    if (scheck.equals(map));{
	        	        Object killerPosition = record._2.get("killedposition");
	        	        String str = (String) killerPosition;
	        	        String[] sarray = str.split(";");
	        	        double[] values = new double[sarray.length];
	        	        for (int i = 0; i < sarray.length; i++)
	        	        	values[i] = Double.parseDouble(sarray[i].split(":")[1]);
	        	        return Vectors.dense(values);
	        	     }
        	        }
        	      }
        	    );
        vectorsRDD.cache();
        
        int numClusters = 10;
        int numIterations = 100;
        KMeansModel clusters = KMeans.train(vectorsRDD.rdd(), numClusters, numIterations);
        
        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
          System.out.println(" " + center);
        }
        
        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(vectorsRDD.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
        
        sc.stop();
        sc.close();

    }
}
