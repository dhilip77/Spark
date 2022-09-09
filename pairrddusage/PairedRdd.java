package pairrddusage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.checkerframework.common.reflection.qual.GetClass;

public class PairedRdd  implements Serializable{

	public static void main(String[] args) throws SparkException {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger logger = Logger.getLogger(GetClass.class.getName());
		
		String fileOne = args[0];
		boolean mode = Boolean.parseBoolean(args[1]);
		
//		List<String> inputData = new ArrayList<>();
//		
//		inputData.add("WARN: Tuesday 4 September 0405");
//		inputData.add("ERROR: Tuesday 4 September 0408");
//		inputData.add("FATAL: Wednesday 5 September 1632");
//		inputData.add("ERROR: Friday 7 September 1854");
//		inputData.add("WARN: Saturday 8 September 1942");
//		
		

		
		//Transformation logic
		
		JavaSparkContext sparkContext = Getsparksession.createSparkSession();
		
		//Read inputFile from args
		JavaRDD<String> inputFile = Getsparksession.Getfileone(fileOne, mode);
		
		//Cache the raw data to in Memory
		JavaRDD<String> logmessage = inputFile;
		
		
		//Print the transformations
		JavaPairRDD<String, String> val = Getsparksession.TransformationBronze(logmessage);
		
		val.collect().forEach(System.out::println);
		
		JavaPairRDD<String, Long> val2 = Getsparksession.TransformationCount(logmessage);
		
		val2.collect().forEach(System.out::println);
		
		JavaPairRDD<String, Long> val3 = Getsparksession.TransformationSums(val2);
		
		val3.foreach(its ->System.out.println(its._1 + " has " + its._2 + " Instances"));
		
		JavaPairRDD<String,Iterable<Long>> val4 = Getsparksession.TransformationGroupBykey(val2);
		
		val4.foreach(tuple -> System.out.println(tuple._1 +" Has " +Iterables.size((tuple._2)) +" Instances." ));
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		
		sparkContext.close();
		
	}

}
