package pairrddusage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;

public class Getsparksession implements Serializable{
	
	private static JavaSparkContext sparkContext;
	private static SparkConf config;
	private static boolean mode;
	
	public static JavaSparkContext createSparkSession() throws SparkException {
		
		config = new SparkConf().setAppName("regularexpression").setMaster("local[3]");
		sparkContext = new JavaSparkContext(config);
		return sparkContext;
	}
	
	public static JavaRDD<String> Getfileone(String filePath, boolean mode) {
		//Read input file from args0
		JavaRDD<String> fileone = sparkContext.textFile(filePath);
		
		List<String> inputData = new ArrayList<>();
		
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		if(mode) {
			return sparkContext.parallelize(inputData);
		}
		
		return fileone;
	}
	
	public static JavaPairRDD<String, String> TransformationBronze(JavaRDD<String> rawRdd){
		
		JavaPairRDD<String, String> pairRdd = rawRdd.mapToPair(raw -> {
			String[] cols = raw.split(":");
			String level = cols[0];
			String date = cols[1];
			
			return new Tuple2<String, String>(level,date);
		
		});
		return pairRdd;
	}
	
	public static JavaPairRDD<String, Long> TransformationCount(JavaRDD<String> rawRdd){
		
		JavaPairRDD<String, Long> pairRdd = rawRdd.mapToPair(raw -> {
			String[] cols = raw.split(":");
			String level = cols[0];
			
			return new Tuple2<>(level, 1L);
			
		});
		return pairRdd;
	}
	
	public static JavaPairRDD<String, Long> TransformationSums(JavaPairRDD<String, Long> rawRdd){
		
		JavaPairRDD<String, Long> pairRdd = rawRdd.reduceByKey((val1, val2) -> val1 + val2);
		
		return pairRdd;
	}

	
	public static JavaPairRDD<String,Iterable<Long>> TransformationGroupBykey(JavaPairRDD<String, Long> rawRdd){
		
		JavaPairRDD<String,Iterable<Long>> pairRdd = rawRdd.groupByKey();
		
		return pairRdd;
	}
}
