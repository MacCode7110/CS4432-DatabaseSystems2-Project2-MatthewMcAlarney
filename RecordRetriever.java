import java.nio.ByteBuffer;
import java.util.*;
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

    public boolean isIndexesCreated() {
        return indexesCreated;
    }

    public void initializeIndexes() throws IOException {
        //Note: A record location in a StringBuilder object is represented using the following notation: File Number (F + #) + "-" + Record Offset (starting index of the record location in the file).
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
        this.indexesCreated = true;
    }

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

        if (this.indexesCreated) {

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

        if (this.indexesCreated) {

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

    public void handleInequalityQueryLookup(int v) throws IOException {
        long startTime = System.currentTimeMillis();
        long endTime;
        StringBuilder matchingRecords;
        matchingRecords = performTableScan(v, -1, false, true);
        endTime = System.currentTimeMillis();
        printQueryResult(matchingRecords, "Table Scan", (endTime - startTime), 99);
    }
}
