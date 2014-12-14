package mdb;
import java.io.File;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

public class TupleDA {
	public TupleDA(EntityStore store) throws DatabaseException {
			// Primary key for SimpleEntityClass classes
			pIdx = store.getPrimaryIndex(String.class, Tuple.class);
			sIdx = store.getSecondaryIndex( pIdx, String.class, "sKey");
					}
			// Index Accessors
			PrimaryIndex<String,Tuple> pIdx;
			SecondaryIndex<String,String,Tuple> sIdx;

}


