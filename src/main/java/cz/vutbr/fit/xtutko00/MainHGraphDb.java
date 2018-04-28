package cz.vutbr.fit.xtutko00;

import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.hbase.hgraphdb.HGraphDbHBaseClient;
import cz.vutbr.fit.xtutko00.utils.Logger;

/**
 * Analyzes data with HGraphDB.
 *
 * @author xtutko00
 */
public class MainHGraphDb {

    private static final Logger logger = new Logger(MainSpark.class);

    private static final String TEST_LONGEST_NAME = "longestName";
    private static final String TEST_TIMESTAMPS = "timestamps";
    private static final String TEST_TIMESTAMPS_SORT = "timestampsSort";
    private static final String TEST_NUMBER_OF_ENTRIES = "numberOfEntries";
    private static final String TEST_SHARED_URLS = "sharedUrls";

    private static final String HBASE_SITE_FILENAME = "/hbase-site.xml";

    /**
     * Main method.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            logger.error("Wrong arguments.");
            printHelp();
            return;
        }

        String tableName = args[0];
        String testName = args[1];

        HBaseClientConfig hBaseClientConfig = new HBaseClientConfig()
                .setTableName(tableName)
                .setHBaseSiteFilename(HBASE_SITE_FILENAME);

        HGraphDbHBaseClient client = new HGraphDbHBaseClient(hBaseClientConfig);

        logger.info("Running test " + testName);
        switch (testName) {
            case TEST_LONGEST_NAME:
                client.testLongestEntryText();
                break;
            case TEST_TIMESTAMPS:
                client.testEntryTimestamps();
                break;
            case TEST_TIMESTAMPS_SORT:
                client.testEntryTimestampsWithSort();
                break;
            case TEST_NUMBER_OF_ENTRIES:
                client.testNumberOfEntries();
                break;
            case TEST_SHARED_URLS:
                client.testSharedUrls();
                break;
            default:
                System.out.println("ERROR: Test name not recognized.");
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Analyzing graph data with HGraphDB");
        System.out.println("Usage: $java -cp twitter-timeline-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.MainHGraphDb [table_name] [test_name]");
        System.out.println("Tests:");
        System.out.println(TEST_LONGEST_NAME + " - get entry with the longest text");
        System.out.println(TEST_TIMESTAMPS + " - get entries and theirs timestamps");
        System.out.println(TEST_TIMESTAMPS_SORT + " - get entries and theirs timestamps and sort newest first");
        System.out.println(TEST_NUMBER_OF_ENTRIES + " - get timelines labels with number of entries");
        System.out.println(TEST_SHARED_URLS + " - get number of occurrences of url");
    }
}
