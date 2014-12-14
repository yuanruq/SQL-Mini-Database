// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;
import Jakarta.util.*;

import java.io.*;
import java.util.*;

import com.sleepycat.je.Transaction;

import static mdb.Main.catalog;
import static mdb.Main.envmnt;
import static mdb.Main.isOpen;
import static mdb.Main.tableCount;
import static mdb.Main.txn;
import static mdb.Main.unCommittedTable;
import static mdb.Main.unCommittedIndex;

public class CommitCmd extends Commit {

    final public static int ARG_LENGTH = 1 /* Kludge! */ ;
    final public static int TOK_LENGTH = 2 ;
    
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
    	for(Transaction x : txn){
    		x.commit();
    	}
    	
    	for(int i=0; i< tableCount; ++i){
    		String tableName = catalog.TableName.get(i);
    		for(int j=0; j<catalog.ColInfo.get(tableName).size(); ++j){
    			if(catalog.isIndexed.get(tableName).get(j).equals(true)){
    				catalog.indexTxn.get(tableName).get(j).commit();
    			}
    		}
    	}
    	/*
    	
    	for(int i=0; i< tableCount; ++i){
    		String tableName = catalog.TableName.get(i);
    		for(int j=0; j<catalog.ColInfo.get(tableName).size(); ++j){
    			catalog.indexTxn.get(tableName).clear();
    		}
    	}*/
    	
    	txn.clear();
    
    	


    	
    	for(int i = 0; i< tableCount; ++i){
    		txn.add(i, envmnt.get(i).beginTransaction(null, null));
    	
    	}
    	
    	for(int i=0; i< tableCount; ++i){
    		String tableName = catalog.TableName.get(i);
    		for(int j=0; j<catalog.ColInfo.get(tableName).size(); ++j){
    			//??????????why?????????
    			catalog.indexTxn.get(tableName).add(j,catalog.indexEnv.get(tableName).get(j).beginTransaction(null, null));
    		
    		}
    	}
    	
    	
    	
    	unCommittedTable.clear();
    	unCommittedIndex.clear();
    	
    	
    }

    public AstToken getCOMMIT () {
        
        return (AstToken) tok [0] ;
    }

    public AstToken getSEMI () {
        
        return (AstToken) tok [1] ;
    }

    public boolean[] printorder () {
        
        return new boolean[] {true, true} ;
    }

    public CommitCmd setParms (AstToken tok0, AstToken tok1) {
        
        arg = new AstNode [ARG_LENGTH] ;
        tok = new AstTokenInterface [TOK_LENGTH] ;
        
        tok [0] = tok0 ;            /* COMMIT */
        tok [1] = tok1 ;            /* SEMI */
        
        InitChildren () ;
        return (CommitCmd) this ;
    }

}
