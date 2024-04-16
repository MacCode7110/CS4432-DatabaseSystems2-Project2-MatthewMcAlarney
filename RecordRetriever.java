import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Hashtable;
import java.lang.StringBuilder;
import java.io.IOException;
import java.io.FileInputStream;

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

    public void initializeIndexes() throws IOException {
        //Note: A record location in a StringBuilder object is represented using the following notation: File Number (F + #) + "-" + Record Offset (starting index of the record location in the file).
        //A comma is used to separate file numbers and corresponding record locations.
        //Example StringBuilder object contents:
        // a. F1-0,F1-40,F1-80
        // b. F2-6,F8-66,F40-33
        this.hashIndex = new Hashtable<Integer, StringBuilder>();
        this.arrayIndex = new StringBuilder[5000];
        String filePath;
        FileInputStream fileInputStream = null;
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
                    this.hashIndex.put(randomVValue, hashIndex.get(randomVValue).append("," + "F").append(i + 1).append("-").append(j));
                }

                if (this.arrayIndex[randomVValue - 1].isEmpty()) {
                    this.arrayIndex[randomVValue - 1] = new StringBuilder("F" + (i + 1) + "-" + j);
                } else {
                    this.arrayIndex[randomVValue - 1] = this.arrayIndex[randomVValue - 1].append("," + "F").append(i + 1).append("-").append(j);
                }

                bytes.clear();
            }
        }
        fileInputStream.close();
    }

    public void handleEqualityQueryLookup(int v) {

    }

    public void handleRangeQueryLookup(int v1, int v2) {

    }

    public void handleInequalityQueryLookup(int v) {

    }
}
