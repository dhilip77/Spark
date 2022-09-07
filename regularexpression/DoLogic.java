package regularexpression;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.checkerframework.common.reflection.qual.GetClass;

import scala.Tuple2;

public class DoLogic implements Serializable {
	
	Logger logger = Logger.getLogger(GetClass.class.getName());
	
	public static JavaRDD<String> convertToWords(JavaRDD<String> fileOneRdd, JavaRDD<String> fileTwoRdd) {
		
//		JavaRDD<String> localRdd = fileOneRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		JavaRDD<String> lettersOnlyRdd = fileOneRdd.map(value -> value.replaceAll("[^a-zA-Z\\s]", " ").toLowerCase());
		
		JavaRDD<String> trimSpaceRdd = lettersOnlyRdd.filter(value -> value.trim().length() > 1); 
		
		JavaRDD<String> wordsOnlyRdd = trimSpaceRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		JavaRDD<String> blanksRemoveRdd = wordsOnlyRdd.filter(value -> value.trim().length() > 0); 
		
		/*
		 * file for word done
		 * now compare two files and get only the matching records
		 */
		Util objectRdd = new Util();
		
		objectRdd.getFile(fileTwoRdd);
		
		JavaRDD<String> resultSet = blanksRemoveRdd.filter(value -> objectRdd.isNonCompared(value));
				
		
		return resultSet;
	}
	
	public static JavaPairRDD<Long, String> countNumWords(JavaRDD<String> BronzeRdd){
		
		JavaPairRDD<String, Long> pairRdd = BronzeRdd.mapToPair(value -> new Tuple2<String, Long>(value, 1L));
		
		JavaPairRDD<String, Long> countRdd = pairRdd.reduceByKey((v,k) -> v + k);
		
		JavaPairRDD<Long,String> switchRdd = countRdd.mapToPair(its -> new Tuple2<Long, String>(its._2,its._1));
		
		JavaPairRDD<Long, String> storeRdd = switchRdd.sortByKey(false);
		
		return storeRdd;
	}

}
