package regularexpression;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.checkerframework.common.reflection.qual.GetClass;

public class GetFiles implements Serializable {
	
	static Logger logger = Logger.getLogger(GetClass.class.getName());
	
	public static JavaSparkContext createSparkSession() throws SparkException {
				
		SparkConf config = new SparkConf().setAppName("regularexpression").setMaster("local[3]");
		JavaSparkContext sparkContext = new JavaSparkContext(config);
		return sparkContext;
	}
//	public static Logger logMessages() {
//	
//		return logger;
//	}
	
	public static JavaRDD<String> getFileOne(JavaSparkContext sparkContext, String path) {
		
		if(path.length() < 0){
			logger.error("File not Found Error..");
			System.exit(404);
		}
		
		JavaRDD<String> fileDataOne = sparkContext.textFile(path);
		
		return fileDataOne;
	}
	
	public static JavaRDD<String> getFileTwo(JavaSparkContext sparkContext, String path) {
		
		if(path.length() < 0){
			logger.error("File not Found Error..");
			
			System.exit(404);
		}
		
		JavaRDD<String> fileDataTwo = sparkContext.textFile(path);
		
		return fileDataTwo;
		
	}

}
