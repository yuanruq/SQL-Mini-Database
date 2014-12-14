// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;
import static mdb.Main.catalog;
import static mdb.Main.dbName;
import Jakarta.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import static mdb.Main.isOpen;
import static mdb.Main.tableCount;
import static mdb.Main.tableIndex;
import static mdb.Main.store;
import static mdb.Main.envmnt;
import static mdb.Main.txn;
import static mdb.Main.unCommittedTable;

public class RelDecl extends Decl_rel {

    final public static int ARG_LENGTH = 2 ;
    final public static int TOK_LENGTH = 5 ;
    private File envHome = null;
    
    public void execute () {
    	if(isOpen==1){
    		execute2();
    	}
    	else{
    		System.out.println("Please open a database first!");
    	}
    }

    public void execute2 () {
        
        //super.execute();
    	 String tableName = getRel_name().tok[0].getTokenName();
    	 unCommittedTable.add(tableName);
    	 
    	 
         if(!catalog.FindTable(tableName)){
           //System.out.println("haha~");
      	   catalog.AddTable(tableName);
      	   
      	   tableIndex.put(tableName, tableCount);
      	   tableCount++;
      	   
      	   envHome = new File("./DataBase/"+dbName+"/startFile");
      	   if(!envHome.exists())
      		   envHome.mkdirs();
      	   
      	   envHome = new File("./DataBase/"+dbName+"/db",tableName);
      	   if(!envHome.exists())
      		   envHome.mkdirs();
      	   
      	 
      	 
      	   
      	   EnvironmentConfig envConfig = new EnvironmentConfig();
      	   StoreConfig storeConfig = new StoreConfig();
      	   envConfig.setAllowCreate(true);
      	   envConfig.setTransactional(true);
      	   storeConfig.setAllowCreate(true);
      	   storeConfig.setTransactional(true);
      	   //envConfig.setTxnTimeout(10000, TimeUnit.MILLISECONDS);
      	   //envConfig.setLockTimeout(10000, TimeUnit.MILLISECONDS);
      	   
      	   envmnt.add(new Environment(envHome, envConfig));
      	   store.add(new EntityStore(envmnt.get(tableCount-1), tableName, storeConfig));
      	   
      	   
      	   txn.add(tableCount-1, envmnt.get(tableCount-1).beginTransaction(null, null));
          
      	   
      	   
      	   
      	   AstNode tmp = getFld_decl_list ().arg[0];
      	   while(tmp!=null){
      		   //System.out.println(tmp.arg[0].arg[0].tok[0].getTokenName());
      		   //System.out.println(tmp.arg[0].arg[1].tok[0].getTokenName());
      		   String featureName = tmp.arg[0].arg[0].tok[0].getTokenName();
      		   String FieldName = tmp.arg[0].arg[1].tok[0].getTokenName();
      		   catalog.AddCol(tableName, featureName);
      		   catalog.AddColField(tableName, FieldName);
      		   catalog.InitialIndex(tableName, false);
      		   
      		   tmp = tmp.right;
      	   }
      	   
      	   if(catalog.indexEnv.get(tableName)==null){
      		   catalog.indexEnv.put(tableName,new ArrayList<Environment>() );
      	   }
      	 
      	   if(catalog.indexStore.get(tableName)==null){
      		   catalog.indexStore.put(tableName,new ArrayList<EntityStore>() );
      	   }
      	
      	   if(catalog.indexTxn.get(tableName)==null){
      		   catalog.indexTxn.put(tableName,new ArrayList<Transaction>() );
      	   }
      	   
      	   for(int i = 0;i<catalog.ColInfo.get(tableName).size();++i){
      		   EnvironmentConfig envConfig2 = new EnvironmentConfig();
        	   StoreConfig storeConfig2 = new StoreConfig();
        	   envConfig2.setAllowCreate(true);
        	   envConfig2.setTransactional(true);
        	   storeConfig2.setAllowCreate(true);
        	   storeConfig2.setTransactional(true);
        	   
      		   String colName = catalog.ColInfo.get(tableName).get(i);
      		   envHome = new File("./DataBase/"+dbName+"/dbIndex/"+tableName,"Index "+" "+colName);
        	   	if(!envHome.exists())
        		   envHome.mkdirs();
        	   catalog.addEnvHome(tableName, envHome);	
        	   catalog.indexEnv.get(tableName).add(new Environment(envHome,envConfig2));
        	   catalog.indexStore.get(tableName).add(new EntityStore(catalog.indexEnv.get(tableName).get(i), tableName, storeConfig2));
        	   catalog.indexTxn.get(tableName).add(catalog.indexEnv.get(tableName).get(i).beginTransaction(null, null));
      	   }
      	   
      	   
      	   
         }
         
         
         else
      	   System.out.println("Table already exist!");
    }

    public AstToken getCREATE () {
        
        return (AstToken) tok [0] ;
    }

    public Fld_decl_list getFld_decl_list () {
        
        return (Fld_decl_list) arg [1] ;
    }

    public AstToken getLP () {
        
        return (AstToken) tok [2] ;
    }

    public AstToken getRP () {
        
        return (AstToken) tok [3] ;
    }

    public Rel_name getRel_name () {
        
        return (Rel_name) arg [0] ;
    }

    public AstToken getSEMI () {
        
        return (AstToken) tok [4] ;
    }

    public AstToken getTABLE () {
        
        return (AstToken) tok [1] ;
    }

    public boolean[] printorder () {
        
        return new boolean[] {true, true, false, true, false, true, true} ;
    }

    public RelDecl setParms
    (AstToken tok0, AstToken tok1, Rel_name arg0, AstToken tok2, Fld_decl_list arg1, AstToken tok3, AstToken tok4)
    {
        
        arg = new AstNode [ARG_LENGTH] ;
        tok = new AstTokenInterface [TOK_LENGTH] ;
        
        tok [0] = tok0 ;            /* CREATE */
        tok [1] = tok1 ;            /* TABLE */
        arg [0] = arg0 ;            /* Rel_name */
        tok [2] = tok2 ;            /* LP */
        arg [1] = arg1 ;            /* Fld_decl_list */
        tok [3] = tok3 ;            /* RP */
        tok [4] = tok4 ;            /* SEMI */
        
        InitChildren () ;
        return (RelDecl) this ;
    }

}
