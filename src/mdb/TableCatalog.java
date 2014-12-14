package mdb;

import static mdb.Main.catalog;
import Jakarta.util.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Stack;
import java.util.EmptyStackException;
import java.util.Vector;
import java.io.*;
import java.util.*;

import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.model.Persistent;


//class used to store table information
public class TableCatalog implements Serializable{
	public ArrayList<String> TableName = new ArrayList<String>();
	public HashMap<String , ArrayList<String>> ColInfo = new HashMap<String , ArrayList<String>>();
	public HashMap<String , ArrayList<String>> ColField = new HashMap<String , ArrayList<String>>();
	public HashMap<String , ArrayList<Boolean>> isIndexed = new HashMap<String , ArrayList<Boolean>>();
		
	public HashMap<String , ArrayList<Environment>> indexEnv = new HashMap<String , ArrayList<Environment>>();;
	public HashMap<String , ArrayList<EntityStore>> indexStore = new HashMap<String , ArrayList<EntityStore>>();
	public HashMap<String , ArrayList<Transaction>> indexTxn = new HashMap<String , ArrayList<Transaction>>();
	public HashMap<String , ArrayList<File>> indexHome = new HashMap<String , ArrayList<File>>();
	
	public boolean FindTable(String Name){
		return TableName.contains(Name);
	}

	public void AddTable(String Name){
		if(FindTable(Name))
			System.out.println("Table already existed");
		else{
			TableName.add(Name);
		}
	}
	
	
	public void AddCol(String T, String Col){
		if (ColInfo.get(T) == null){
			ColInfo.put(T, new ArrayList<String>());

		}
		ColInfo.get(T).add(Col);
			
	}
	
	public void AddColField(String T, String Col){
		if (ColField.get(T) == null){
			ColField.put(T, new ArrayList<String>());
		}
		ColField.get(T).add(Col);
	}
	
	public void InitialIndex(String T, Boolean X){
		if(isIndexed.get(T) == null){
			isIndexed.put(T, new ArrayList<Boolean>());
		}
		isIndexed.get(T).add(X);
	}
	
	public void SetIndex(String T, int pos, Boolean X){
		isIndexed.get(T).set(pos, X);
	}
	
	public void SetIndexEnv(String T, int pos, Environment e){
		indexEnv.get(T).set(pos, e);
	}
	
	public void SetIndexStore(String T, int pos, EntityStore s){
		indexStore.get(T).set(pos, s);
	}
	
	public void SetIndexTransaction(String T, int pos, Transaction t){
		indexTxn.get(T).set(pos, t);
	}
	
	public File getEnvHome(String T, int pos){
		return indexHome.get(T).get(pos);
	}
	
	public void addEnvHome(String T, File f){
		if(indexHome.get(T)==null){
			indexHome.put(T, new ArrayList<File>());
		}
		indexHome.get(T).add(f);
	}
	


	
	
	
	
	
	public void Show(){
		//System.out.println("Table Information is as follows");
		for(int i = 0; i <TableName.size();++i){
			String temp = TableName.get(i); 
			System.out.print(temp +"(");
			ArrayList<String> temp2 = ColInfo.get(temp);
			for (int j = 0; j<temp2.size();++j){
				System.out.print(temp2.get(j).toString());
				if(j!=temp2.size()-1)
					System.out.print(",");
			}
			System.out.println(")");
			
		}
	}
	
	
	
}
