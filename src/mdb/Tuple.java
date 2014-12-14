package mdb;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

import java.util.*;

@Entity
public class Tuple {
	@PrimaryKey
	private String pKey;
	//Secondary key is the sKey
	@SecondaryKey(relate=MANY_TO_ONE)
	private String sKey;
	
	private String tableName;
	private ArrayList<String> singleTuple;
	
	
	
	
	
	public void setPKey() {
		long now = System.nanoTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
		pKey = dateFormat.format( now ); 
	}
	
	public void setSKey(String data) {
	sKey = data;
	}
	
	public String getPKey() {
	return pKey;
	}
	
	public String getSKey() {
	return sKey;
	}
	
	public void setTableName(String name){
		tableName = name;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public void setValue(ArrayList<String> X){
		singleTuple = new ArrayList<String>(X);
	}
	
	public ArrayList<String> getValue(){
		return singleTuple;
	}
	
	

}
