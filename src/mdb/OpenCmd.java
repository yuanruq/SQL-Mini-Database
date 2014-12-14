// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;
import Jakarta.util.*;
import static mdb.Main.isOpen;
import static mdb.Main.dbName;
import static mdb.Main.catalog;
import static mdb.Main.envmnt;
import static mdb.Main.store;
import static mdb.Main.tableCount;
import static mdb.Main.tableIndex;
import static mdb.Main.txn;

import java.io.*;
import java.util.*;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

public class OpenCmd extends Open {

    final public static int ARG_LENGTH = 1 /* Kludge! */ ;
    final public static int TOK_LENGTH = 3 ;
    private File dbHome = null;
    private File startHome = null;
    private Environment startEnv;
    private EntityStore startStore;

    public void execute () {
        
        //super.execute();
    	dbName = getSTRING_LITERAL ().getTokenName();
    	dbHome = new File("./DataBase",dbName);
   	   	if(!dbHome.exists()){
   		   dbHome.mkdirs();
   	   	}
   	   
   	   	else{ 
   	   		
   	   	    startHome = new File("./DataBase/"+dbName+"/startFile");
   	   	   	if(!startHome.exists()){
   	   		   System.out.println("Start file missing!");
   	   		   return;
   	   		}
   	   	   	
   	   	   	ObjectInputStream inputStream = null;
   	   	   	try{
   	   	   		inputStream = new ObjectInputStream(new FileInputStream("./DataBase/"+dbName+"/startFile/start.txt"));
   	   	   		Object obj = null;
   	   	   		while ((obj = inputStream.readObject()) != null) {
   	   	   			if (obj instanceof StartData){
   	   	   				
   	   	   				tableCount=((StartData)obj).getTableCount();
   	   	   				tableIndex = ((StartData)obj).getTableIndex();
   	   	   				catalog.ColField = ((StartData)obj).getColField();
   	   	   				catalog.ColInfo = ((StartData)obj).getColInfo();
   	   	   				catalog.TableName = ((StartData)obj).getTableName();
   	   	   				catalog.isIndexed = ((StartData)obj).GetIsIndexed();
   	   	   			}
   	   	   		}
   	   	   	}catch (EOFException ex) { 
   	            //System.out.println("End of file reached.");
   	        } catch (ClassNotFoundException ex) {
   	            ex.printStackTrace();
   	        } catch (FileNotFoundException ex) {
   	            ex.printStackTrace();
   	        } catch (IOException ex) {
   	            ex.printStackTrace();
   	        }finally {
   	            //Close the ObjectInputStream
   	            try {
   	                if (inputStream != null) {
   	                    inputStream.close();
   	                }
   	            } catch (IOException ex) {
   	                ex.printStackTrace();
   	            }
   	        }
   	   	   	
   	   	   	
   	   	   	for(int i=0;i<catalog.TableName.size();++i){
   	   	   		String tableName = catalog.TableName.get(i);
   	   	   		File envHome = null;
   	   	   		
   	   	   		envHome = new File("./DataBase/"+dbName+"/db",tableName);
   	   	   		
   	   	   		EnvironmentConfig envConfig = new EnvironmentConfig();
   	   	   		StoreConfig storeConfig = new StoreConfig();
   	   	   		envConfig.setAllowCreate(true);
   	   	   		envConfig.setTransactional(true);
   	   	   		storeConfig.setAllowCreate(true);
   	   	   		storeConfig.setTransactional(true);
   	   	   		//envConfig.setTxnTimeout(10000, TimeUnit.MILLISECONDS);
   	   	   		//envConfig.setLockTimeout(10000, TimeUnit.MILLISECONDS);
     	   
   	   	   		envmnt.add(new Environment(envHome, envConfig));
   	   	   		store.add(new EntityStore(envmnt.get(i), tableName, storeConfig));
   	   	   		txn.add(i, envmnt.get(i).beginTransaction(null, null));
   	   	   		
   	   	   		if(catalog.indexEnv.get(tableName)==null){
   	   	   			catalog.indexEnv.put(tableName,new ArrayList<Environment>() );
   	   	   		}
     	 
   	   	   		if(catalog.indexStore.get(tableName)==null){
   	   	   			catalog.indexStore.put(tableName,new ArrayList<EntityStore>() );
   	   	   		}
     	
   	   	   		if(catalog.indexTxn.get(tableName)==null){
   	   	   			catalog.indexTxn.put(tableName,new ArrayList<Transaction>() );
   	   	   		}
   	   	   		
   	   	   		for(int j = 0;j<catalog.ColInfo.get(tableName).size();++j){
   	   	   			EnvironmentConfig envConfig2 = new EnvironmentConfig();
   	   	   			StoreConfig storeConfig2 = new StoreConfig();
   	   	   			envConfig2.setAllowCreate(true);
   	   	   			envConfig2.setTransactional(true);
   	   	   			storeConfig2.setAllowCreate(true);
   	   	   			storeConfig2.setTransactional(true);
       	   
   	   	   			String colName = catalog.ColInfo.get(tableName).get(j);
   	   	   			envHome = new File("./DataBase/"+dbName+"/dbIndex/"+tableName,"Index "+" "+colName);
   	   	   			
   	   	   			catalog.addEnvHome(tableName, envHome);	
   	   	   			catalog.indexEnv.get(tableName).add(new Environment(envHome,envConfig2));
   	   	   			catalog.indexStore.get(tableName).add(new EntityStore(catalog.indexEnv.get(tableName).get(j), tableName, storeConfig2));
   	   	   			catalog.indexTxn.get(tableName).add(catalog.indexEnv.get(tableName).get(j).beginTransaction(null, null));
   	   	   		}
   	   	   	}
   	   	   	
   	   	   
   	   		
   	   		
   	   	}
   	   	isOpen = 1;
   	   	
   	
    	
    }

    public AstToken getOPEN () {
        
        return (AstToken) tok [0] ;
    }

    public AstToken getSEMI () {
        
        return (AstToken) tok [2] ;
    }

    public AstToken getSTRING_LITERAL () {
        
        return (AstToken) tok [1] ;
    }

    public boolean[] printorder () {
        
        return new boolean[] {true, true, true} ;
    }

    public OpenCmd setParms (AstToken tok0, AstToken tok1, AstToken tok2) {
        
        arg = new AstNode [ARG_LENGTH] ;
        tok = new AstTokenInterface [TOK_LENGTH] ;
        
        tok [0] = tok0 ;            /* OPEN */
        tok [1] = tok1 ;            /* STRING_LITERAL */
        tok [2] = tok2 ;            /* SEMI */
        
        InitChildren () ;
        return (OpenCmd) this ;
    }

}