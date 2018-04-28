package cz.vutbr.fit.xtutko00;

import cz.vutbr.fit.xtutko00.hbase.halyard.HalyardHBaseClient;
import cz.vutbr.fit.xtutko00.utils.Logger;

/**
 * Analyzes data with HGraphDB.
 *
 * @author xtutko00
 */
public class MainHalyard {

    private static final Logger logger = new Logger(MainSpark.class);

    private static final String TEST_LONGEST_TEXT = "longestText";
    private static final String TEST_TIMESTAMPS = "timestamps";
    private static final String TEST_TIMESTAMPS_SORT = "timestampsSort";
    private static final String TEST_NUMBER_OF_ENTRIES = "numberOfEntries";
    private static final String TEST_SHARED_URLS = "sharedUrls";

    /**
     * Main method.
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            logger.error("Wrong arguments.");
            printHelp();
            return;
        }

        String serverUrl = args[0];
        String repositoryName = args[1];
        String testName = args[2];

        HalyardHBaseClient halyardHBaseClient = new HalyardHBaseClient();

        logger.info("Running test " + testName);
        switch (testName) {
            case TEST_LONGEST_TEXT:
                halyardHBaseClient.testLongestEntryText(serverUrl, repositoryName);
                break;
            case TEST_TIMESTAMPS:
                halyardHBaseClient.testEntryTimestamps(serverUrl, repositoryName);
                break;
            case TEST_TIMESTAMPS_SORT:
                halyardHBaseClient.testEntryTimestampsWithSort(serverUrl, repositoryName);
                break;
            case TEST_NUMBER_OF_ENTRIES:
                halyardHBaseClient.testNumberOfEntries(serverUrl, repositoryName);
                break;
            case TEST_SHARED_URLS:
                halyardHBaseClient.testSharedUrls(serverUrl, repositoryName);
                break;
            default:
                System.out.println("ERROR: Test name not recognized.");
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Analyzing graph data with Halyard");
        System.out.println("Usage: $java -cp twitter-timeline-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.MainHalyard [server_url] [repository_name] [test_name]");
        System.out.println("Tests:");
        System.out.println(TEST_LONGEST_TEXT + " - get entry with the longest text");
        System.out.println(TEST_TIMESTAMPS + " - get entries and theirs timestamps");
        System.out.println(TEST_TIMESTAMPS_SORT + " - get entries and theirs timestamps and sort newest first");
        System.out.println(TEST_NUMBER_OF_ENTRIES + " - get timelines labels with number of entries");
        System.out.println(TEST_SHARED_URLS + " - get number of occurrences of url");
    }
}
