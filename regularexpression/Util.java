package regularexpression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;

public class Util implements Serializable{
	
	private static Set<String> listSet = new HashSet<>();
	
	//public static List<String> listSet = new ArrayList<>();
	
	
	
	public void getFile(JavaRDD<String> files){
		
		files.collect().forEach(listSet::add);
		
	}
	
	public boolean isListed(String value) {
		
		return listSet.contains(value);
	}
	
//	public boolean isCompared(String Value) {
//		
//		return mySet.contains(Value);
//	}
//	
	public boolean isNonCompared(String Value) {
		
		return !isListed(Value);
	}
	

}
