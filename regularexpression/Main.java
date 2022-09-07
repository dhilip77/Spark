package regularexpression;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.checkerframework.common.reflection.qual.GetClass;

import scala.Tuple2;


public class Main implements Serializable {

	public static void main(String[] args) throws SparkException {
		// TODO Auto-generated method stub
		
		Logger logger = Logger.getLogger(GetClass.class.getName());
				
		if(args[0].length() < 0 && args[1].length() < 0) {
			
			logger.error("USAGE:: FILES ARGUMENTS PATH MISSING OR FILE NOT FOUND");
			System.exit(2);
		}
			
		JavaSparkContext spark = GetFiles.createSparkSession();
		try {
			
			
			String fileDataOnePath = args[0];
			String fileDataTwoPath = args[1];
			
			JavaRDD<String> fileOneRdd = GetFiles.getFileOne(spark, fileDataOnePath);
			JavaRDD<String> fileTwoRdd = GetFiles.getFileTwo(spark, fileDataTwoPath);
			
			JavaRDD<String> resultSet = DoLogic.convertToWords(fileOneRdd, fileTwoRdd);
			
			logger.info("Spark Usage Started with File Read...");
			
			//resultSet.take(10).forEach(System.out::println);
			///home/dhiliptg/eclipse-spring/javaspark/resources/inp*.txt
			
			JavaPairRDD<Long, String> resultSet2 = DoLogic.countNumWords(resultSet);
			
			List<Tuple2<Long, String>> resultList = resultSet2.take(20);
			
			resultList.forEach(System.out::println);
			
			
		}
		catch(ArrayIndexOutOfBoundsException except) {
			
			logger.error(except +":: FILE USAGE PATH ARGUMENTS NOT FOUND");
			System.exit(1);
		}
		catch(Exception except) {
			logger.error(except+":: MAIN PROGRAM EXCEPTION");
		}

		finally {
		
		GetFiles.createSparkSession().close();

		}
	}
}
