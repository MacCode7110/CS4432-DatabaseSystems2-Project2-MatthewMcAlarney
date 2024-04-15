import java.util.Hashtable;
import java.lang.StringBuilder;

public class RecordRetriever {
    private boolean indexesCreated;
    private Hashtable<Integer, StringBuilder> hashIndex;
    private StringBuilder[] arrayIndex;
    public RecordRetriever() {
        this.indexesCreated = false;
    }

    public void setIndexesCreated(boolean indexesCreated) {
        this.indexesCreated = indexesCreated;
    }

    public boolean isIndexesCreated() {
        return indexesCreated;
    }

    public void initializeHashIndex() {
        //A hash table entry should have two
        //components (key k, value v), where k = RandomV value, and v = the record locations (file number and the offset at
        //which the record begins within this file).

    }

    public void initializeArrayIndex() {

    }
    public void handleEqualityQueryLookup(int v) {

    }

    public void handleRangeQueryLookup(int v1, int v2) {

    }

    public void handleInequalityQueryLookup(int v) {

    }
}
