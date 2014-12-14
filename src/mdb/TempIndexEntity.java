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
public class TempIndexEntity {
	
	@PrimaryKey
	private String pKey;
	
	@SecondaryKey(relate=MANY_TO_ONE)
	private String stringKey;
	
	@SecondaryKey(relate=MANY_TO_ONE)
	private int intKey;
	
	private String tableName;
	
	public void setPKey(String s) {
		pKey = s;
	}
	
	public String getPKey() {
		return pKey;
	}
	
	public void setStringKey(String data) {
		stringKey = data;
	}
	
	public void setIntKey(int data) {
		intKey = data;
	}
	
	public String getStringKey() {
		return stringKey;
	}
	
	public int getIntKey(int data) {
		return intKey;
	}
	
	public void setTableName(String name){
		tableName = name;
	}
	
	public String getTableName(){
		return tableName;
	}
	
	}
