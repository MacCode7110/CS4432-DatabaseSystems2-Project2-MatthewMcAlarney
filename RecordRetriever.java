import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Hashtable;
import java.lang.StringBuilder;
import java.io.IOException;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RecordRetriever {
    private final static int TOTALNUMBEROFFILES = 99;
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
    public void initializeHashIndex() throws IOException {
        //Note: A record location in a StringBuilder object is represented using the following notation: File Number (F + #) + "-" + Record Offset (starting index of the record location in the file).
        //A dash is used to separate multiple locations of a record in a given file.
        //Example StringBuilder object contents: F1-0-40-80
        this.hashIndex = new Hashtable<Integer, StringBuilder>();
        String filePath;
        FileInputStream fileInputStream;
        byte[] record;
        int randomVValue;
        ByteBuffer bytes = ByteBuffer.allocate(40);
        for (int i = 0; i < TOTALNUMBEROFFILES; i++) {
            filePath = "Project2Dataset/F" + (i + 1) + ".txt";
            fileInputStream = new FileInputStream(filePath);
            for (int j = 0; j < 4000; j+= 40) {
                fileInputStream.getChannel().read(bytes, j);
                record = bytes.array();
                randomVValue = Integer.parseInt(new String(Arrays.copyOfRange(record, j + 33, j + 37)));

                if (!(this.hashIndex.contains(randomVValue))) {
                    this.hashIndex.put(randomVValue, new StringBuilder("F" + (i + 1) + "-" + j));
                } else {
                    this.hashIndex.put(randomVValue, hashIndex.get(randomVValue).append("-" + j));
                }

                bytes.clear();
            }
        }
    }

    public void initializeArrayIndex() {
        this.arrayIndex = new StringBuilder[5000];
    }
    public void handleEqualityQueryLookup(int v) {

    }

    public void handleRangeQueryLookup(int v1, int v2) {

    }

    public void handleInequalityQueryLookup(int v) {

    }
}
