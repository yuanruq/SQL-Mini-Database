package mdb;
import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

public class IndexTupleDA {
	public IndexTupleDA(EntityStore store) throws DatabaseException {
		// Primary key for SimpleEntityClass classes
		pIdx = store.getPrimaryIndex(String.class, TempIndexEntity.class);
		sStringIdx = store.getSecondaryIndex( pIdx, String.class, "stringKey");
		sIntIdx = store.getSecondaryIndex( pIdx, Integer.class, "intKey");
				}
		// Index Accessors
		PrimaryIndex<String,TempIndexEntity> pIdx;
		SecondaryIndex<String,String,TempIndexEntity> sStringIdx;
		SecondaryIndex<Integer, String,TempIndexEntity> sIntIdx;

}
