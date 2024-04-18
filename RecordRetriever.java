import java.nio.ByteBuffer;
import java.util.*;
import java.lang.StringBuilder;
import java.io.IOException;
import java.io.FileInputStream;

/**
 * Retrieves records from the Project2Dataset directory through index-lookups and full table scans
 * Answers equality queries, range queries, and inequality queries
 */
public class RecordRetriever {

    /**
     * The total number of files in the Project2Dataset directory
     */
    private final static int TOTALNUMBEROFFILES = 99;

    /**
     * A boolean that indicates whether the hash-based and array-based indexes are created
     */
    private boolean indexesInitialized;

    /**
     * The hash-based index serving equality queries
     */
    private Hashtable<Integer, StringBuilder> hashIndex;

    /**
     * The array-based index serving range queries
     */
    private StringBuilder[] arrayIndex;

    /**
     * RecordRetriever constructor that sets indexesCreated field to false
     */
    public RecordRetriever() {
        this.indexesInitialized = false;
    }

    /**
     * Returns a boolean indicating whether the hash-based and array-based indexes are initialized
     * @return a boolean indicating whether the hash-based and array-based indexes are initialized
     */
    public boolean isIndexesInitialized() {
        return indexesInitialized;
    }

    /**
     * Initializes the hash-based and array-based indexes.
     * @throws IOException
     */
    public void initializeIndexes() throws IOException {
        //A record location in a StringBuilder object is represented using the following notation: File Number (F + #) + "-" + Record Offset (starting index of the record location in the file).
        //A comma is used to separate file numbers and corresponding record locations.
        //Example StringBuilder object contents:
        // a. F1-0,F1-40,F1-80
        // b. F2-6,F8-66,F40-33
        this.hashIndex = new Hashtable<>();
        this.arrayIndex = new StringBuilder[5000];
        FileInputStream fileInputStream = null;
        int randomVValue;
        ByteBuffer bytes = ByteBuffer.allocate(40);

        for (int i = 0; i < TOTALNUMBEROFFILES; i++) {

            fileInputStream = new FileInputStream("Project2Dataset/F" + (i + 1) + ".txt");

            for (int j = 0; j < 4000; j+= 40) {

                fileInputStream.getChannel().read(bytes, j);
                randomVValue = Integer.parseInt(new String(Arrays.copyOfRange(bytes.array(), 33, 37)));

                if (!(this.hashIndex.containsKey(randomVValue))) {
                    this.hashIndex.put(randomVValue, new StringBuilder("F" + (i + 1) + "-" + j));
                } else {
                    this.hashIndex.put(randomVValue, hashIndex.get(randomVValue).append("," + "F").append(i + 1).append("-").append(j));
                }

                if (this.arrayIndex[randomVValue - 1] == null) {
                    this.arrayIndex[randomVValue - 1] = new StringBuilder("F" + (i + 1) + "-" + j);
                } else {
                    this.arrayIndex[randomVValue - 1] = this.arrayIndex[randomVValue - 1].append("," + "F").append(i + 1).append("-").append(j);
                }

                bytes.clear();
            }
        }

        fileInputStream.close();
        this.indexesInitialized = true;
    }

    /**
     * Performs a full table scan on the files under the Project2Dataset directory.
     * @param v1 the first specified randomV value (this is the value that is always used for equality-based queries and is the lower bound (exclusive) for range-based queries)
     * @param v2 the second specified randomV value (this is the value that is always used as the upper bound (exclusive) for range-based queries. In the case of equality-based queries, -1 is passed to v2)
     * @param isRangeQuery whether a range-based query is performed
     * @param isInequalityQuery whether an inequality-based query is performed
     * @return the records that fulfill the query
     * @throws IOException
     */
    public StringBuilder performTableScan(int v1, int v2, boolean isRangeQuery, boolean isInequalityQuery) throws IOException {
        StringBuilder matchingRecords = new StringBuilder();
        FileInputStream fileInputStream = null;
        int randomVValue;
        ByteBuffer bytes = ByteBuffer.allocate(40);

        for (int i = 0; i < TOTALNUMBEROFFILES; i++) {

            fileInputStream = new FileInputStream("Project2Dataset/F" + (i + 1) + ".txt");

            for (int j = 0; j < 4000; j+=40) {

                fileInputStream.getChannel().read(bytes, j);
                randomVValue = Integer.parseInt(new String(Arrays.copyOfRange(bytes.array(), 33, 37)));

                if (isRangeQuery) {
                    if (randomVValue > v1 && randomVValue < v2) {
                        matchingRecords.append(new String(bytes.array())).append(",");
                    }
                } else if (isInequalityQuery) {
                    if (randomVValue != v1) {
                        matchingRecords.append(new String(bytes.array())).append(",");
                    }
                } else {
                    if (randomVValue == v1) {
                        matchingRecords.append(new String(bytes.array())).append(",");
                    }
                }

                bytes.clear();
            }
        }

        fileInputStream.close();
        return matchingRecords;
    }

    /**
     * Prints the result of a query, which includes details about the records that fulfill the query, how the records were retrieved, how long it took to answer the query, and the number of distinct files read
     * @param matchingRecords the records that fulfill the query
     * @param retrievalMethod the method used to retrieve the records (index lookup vs. full table scan)
     * @param queryTime total time needed to answer the query
     * @param numberOfFilesRead the number of distinct files read
     */
    public void printQueryResult(StringBuilder matchingRecords, String retrievalMethod, long queryTime, int numberOfFilesRead) {
        String recordsResult;

        if (matchingRecords.length() == 0) {
            recordsResult = "None";
        } else {
            recordsResult = matchingRecords.substring(0, matchingRecords.length() - 1); //Remove the extra trailing comma from all matching records results.
        }

        System.out.println("Matching records: " + recordsResult + "\n"
                + retrievalMethod + "\n"
                + "Time to answer query: " + queryTime + " milliseconds" + "\n"
                + "Number of files read: " + numberOfFilesRead);
    }

    /**
     * Answers equality-based queries through a hash-based index lookup or full table scan
     * @param v the randomV value to search for in the hash-based index or through a full table scan
     * @throws IOException
     */
    public void handleEqualityQueryLookup(int v) throws IOException {
        long startTime = System.currentTimeMillis();
        long endTime;
        String retrievalMethod;
        int numberOfFilesRead = 0;
        String[] recordLocations;
        int fileNumber = 1;
        int previousFileNumber = 1;
        FileInputStream fileInputStream = null;
        ByteBuffer bytes = ByteBuffer.allocate(40);
        StringBuilder matchingRecords =  new StringBuilder();

        if (this.indexesInitialized) {

            retrievalMethod = "Hash-based Index";

            if (this.hashIndex.containsKey(v)) {

                recordLocations = this.hashIndex.get(v).toString().split(",");

                for (int i = 0; i < recordLocations.length; i++) {

                    if (i > 0) {
                        previousFileNumber = fileNumber;
                    }

                    fileNumber = Integer.parseInt(recordLocations[i].substring(1, recordLocations[i].indexOf("-", 1)));

                    if (i == 0 || fileNumber > previousFileNumber) {
                        fileInputStream = new FileInputStream("Project2Dataset/F" + fileNumber + ".txt");
                        numberOfFilesRead++;
                    }

                    fileInputStream.getChannel().read(bytes, Integer.parseInt(recordLocations[i].substring((recordLocations[i].indexOf("-") + 1))));
                    matchingRecords.append(new String(bytes.array())).append(",");
                    bytes.clear();
                }

                assert fileInputStream != null;
                fileInputStream.close();
            }
        } else {

            retrievalMethod = "Table Scan";
            numberOfFilesRead = 99;
            matchingRecords = performTableScan(v, -1, false, false);

        }
        endTime = System.currentTimeMillis();
        printQueryResult(matchingRecords, retrievalMethod, (endTime - startTime), numberOfFilesRead);
    }

    /**
     * Answers range-based queries through an array-based index lookup or full table scan
     * @param v1 the first specified randomV value (this is the value that is used for the lower bound (exclusive) for range-based queries)
     * @param v2 the second specified randomV value (this is the value that is used as the upper bound (exclusive) for range-based queries)
     * @throws IOException
     */
    public void handleRangeQueryLookup(int v1, int v2) throws IOException {
        long startTime = System.currentTimeMillis();
        long endTime;
        String retrievalMethod;
        Hashtable<Integer, TreeSet<Integer>> fileRecordMap = new Hashtable<>();
        TreeSet<Integer> placeholderTreeSet;
        String[] recordLocations;
        Set<Map.Entry<Integer, TreeSet<Integer>>> entrySet;
        int fileNumber;
        int numberOfFilesRead = 0;
        FileInputStream fileInputStream = null;
        ByteBuffer bytes = ByteBuffer.allocate(40);
        StringBuilder matchingRecords = new StringBuilder();

        if (this.indexesInitialized) {

            retrievalMethod = "Array-based Index";

            if (v1 >= 1 && v1 <= 5000 && v2 >= 1 && v2 <= 5000) {

                for (int i = v1; i < (v2 - 1); i++) {

                    if (!(this.arrayIndex[i] == null)) {

                        recordLocations = this.arrayIndex[i].toString().split(",");

                        for (int j = 0; j < recordLocations.length; j++) {
                            fileNumber = Integer.parseInt(recordLocations[j].substring(1, recordLocations[j].indexOf("-", 1)));

                            if (!(fileRecordMap.containsKey(fileNumber))) {
                                fileRecordMap.put(fileNumber, new TreeSet<>());
                            }

                            placeholderTreeSet = fileRecordMap.get(fileNumber);
                            placeholderTreeSet.add(Integer.parseInt(recordLocations[j].substring((recordLocations[j].indexOf("-") + 1))));
                            fileRecordMap.put(fileNumber, placeholderTreeSet);
                        }
                    }
                }

                entrySet = fileRecordMap.entrySet();

                for (Map.Entry<Integer, TreeSet<Integer>> entry : entrySet) {

                    fileInputStream = new FileInputStream("Project2Dataset/F" + entry.getKey() + ".txt");

                    for (Integer i : entry.getValue()) {
                        fileInputStream.getChannel().read(bytes, i);
                        matchingRecords.append(new String(bytes.array())).append(",");
                        bytes.clear();
                    }

                    numberOfFilesRead++;
                }

                assert fileInputStream != null;
                fileInputStream.close();
            }
        } else {
            retrievalMethod = "Table Scan";
            numberOfFilesRead = 99;
            matchingRecords = performTableScan(v1, v2, true, false);
        }
        endTime = System.currentTimeMillis();
        printQueryResult(matchingRecords, retrievalMethod, (endTime - startTime), numberOfFilesRead);
    }

    /**
     * Answers inequality-based queries through a full table scan
     * @param v the randomV value to search for through a full table scan
     * @throws IOException
     */
    public void handleInequalityQueryLookup(int v) throws IOException {
        long startTime = System.currentTimeMillis();
        long endTime;
        StringBuilder matchingRecords;
        matchingRecords = performTableScan(v, -1, false, true);
        endTime = System.currentTimeMillis();
        printQueryResult(matchingRecords, "Table Scan", (endTime - startTime), 99);
    }
}
