// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;
import Jakarta.util.*;
import java.io.*;
import java.util.*;

public class WherePredStmt extends WherePred {

    final public static int ARG_LENGTH = 1 ;
    final public static int TOK_LENGTH = 1 ;

    public void execute () {
        
        super.execute();
    }

    public Pred getPred () {
        
        return (Pred) arg [0] ;
    }

    public AstToken getWHERE () {
        
        return (AstToken) tok [0] ;
    }

    public boolean[] printorder () {
        
        return new boolean[] {true, false} ;
    }

    public WherePredStmt setParms (AstToken tok0, Pred arg0) {
        
        arg = new AstNode [ARG_LENGTH] ;
        tok = new AstTokenInterface [TOK_LENGTH] ;
        
        tok [0] = tok0 ;            /* WHERE */
        arg [0] = arg0 ;            /* Pred */
        
        InitChildren () ;
        return (WherePredStmt) this ;
    }

}
