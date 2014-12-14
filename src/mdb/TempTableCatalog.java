package mdb;

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
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Stack;
import java.util.EmptyStackException;
import java.util.Vector;
import java.io.*;
import java.util.*;

public class TempTableCatalog {
	private ArrayList<String> TableName = new ArrayList<String>();
	private HashMap<String , ArrayList<String>> ColInfo = new HashMap<String , ArrayList<String>>();
	private HashMap<String , ArrayList<String>> ColField = new HashMap<String , ArrayList<String>>();
	
	
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
	
	public ArrayList<String> GetTableList(){
		return TableName;
	}
	
	public HashMap<String , ArrayList<String>> GetColInfo(){
		return ColInfo;
	}
	
	public HashMap<String , ArrayList<String>> GetColField(){
		return ColField;
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
