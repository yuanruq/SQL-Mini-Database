package mdb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

public class StartData implements Serializable{
	
	
	private int tableCount;
	private HashMap<String, Integer> tableIndex = new HashMap<String , Integer>();
	private ArrayList<String> TableName = new ArrayList<String>();
	private HashMap<String , ArrayList<String>> ColInfo = new HashMap<String , ArrayList<String>>();
	private HashMap<String , ArrayList<String>> ColField = new HashMap<String , ArrayList<String>>();
	private HashMap<String , ArrayList<Boolean>> isIndexed = new HashMap<String , ArrayList<Boolean>>();

	
	
	public void setTableCount(int t){
		tableCount = t;
	}
	
	public int getTableCount(){
		return tableCount;
	}
	
	public void setTableIndex(HashMap<String, Integer> t){
		tableIndex = t;
	}
	
	public HashMap<String, Integer> getTableIndex(){
		return tableIndex;
	}
	
	public void setTableName(ArrayList<String> t){
		TableName = t;
	}
	
	public ArrayList<String> getTableName(){
		return TableName;
	}
	
	public void setColInfo(HashMap<String , ArrayList<String>> t){
		ColInfo = t;
	}
	
	public HashMap<String , ArrayList<String>> getColInfo(){
		return ColInfo;
	}
	
	public void setColField(HashMap<String , ArrayList<String>> t){
		ColField = t;
	}
	
	public HashMap<String , ArrayList<String>> getColField(){
		return ColField;
	}
	
	public void SetIsIndexed(HashMap<String , ArrayList<Boolean>> t){
		isIndexed = t;
	}
	
	public HashMap<String , ArrayList<Boolean>> GetIsIndexed(){
		return isIndexed;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	

}
