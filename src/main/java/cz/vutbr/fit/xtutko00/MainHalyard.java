package cz.vutbr.fit.xtutko00;

import cz.vutbr.fit.xtutko00.hbase.halyard.HalyardHBaseClient;
import cz.vutbr.fit.xtutko00.utils.Logger;

public class MainHalyard {

    private static final Logger logger = new Logger(MainSpark.class);

    private static final String TEST_LONGEST_NAME = "longestName";
    private static final String TEST_TIMESTAMPS = "timestamps";

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

        switch (testName) {
            case TEST_LONGEST_NAME:
                halyardHBaseClient.testLongestEntryText(serverUrl, repositoryName);
                break;
            case TEST_TIMESTAMPS:
                if (args.length < 4) {
                    logger.error("No timeline label given. Please set timeline label as last parameter.");
                    break;
                }
                String timelineLabel = args[3];
                halyardHBaseClient.testEntryTimestamps(serverUrl, repositoryName, timelineLabel);
                break;
            default:
                System.out.println("ERROR: Test name not recognized.");
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Analyzing graph data with Halyard");
        System.out.println("Usage: $java -cp twitter-timeline-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.MainHalyard [server_url] [repository_name] [test_name] [additional_args]");
        System.out.println("Tests:");
        System.out.println(TEST_LONGEST_NAME + " - get entry with the longest text");
        System.out.println(TEST_TIMESTAMPS + " - get entries and theirs timestamps (additional argument [timeline_label] needed)");
    }
}
