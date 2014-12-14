package mdb;
import java.util.*;

public class TempTuple {
	private ArrayList<String> tableName = new ArrayList<String>();
	//public HashMap<String , ArrayList<String>> records = new HashMap<String , ArrayList<String>>();
	//public ArrayList<String> stringRecords = new ArrayList<String>();;
	private HashMap<String , ArrayList<ArrayList<String>>> records = new HashMap<String , ArrayList<ArrayList<String>>>();
	
	public boolean FindTable(String Name){
		return tableName.contains(Name);
	}
	
	public void AddTable(String Name){
		if(!FindTable(Name))
			tableName.add(Name);
	}
	
	public void AddValue(String T, ArrayList<String> X){
		if(records.get(T) == null){
			records.put(T, new ArrayList<ArrayList<String>>());
		}
		records.get(T).add(X);
		
	}
	
	public void Initalize(String T){
		if(records.get(T) == null){
			records.put(T, new ArrayList<ArrayList<String>>());
		}
		
	}
	
	public HashMap<String , ArrayList<ArrayList<String>>> GetValue(){
		return records;
	}
	
	

}






	
	
	
	
	
	
	
	
