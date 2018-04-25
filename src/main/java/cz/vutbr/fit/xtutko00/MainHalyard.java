package cz.vutbr.fit.xtutko00;

import cz.vutbr.fit.xtutko00.hbase.HBaseClientFactory;
import cz.vutbr.fit.xtutko00.hbase.halyard.HalyardHBaseClient;

public class MainHalyard {

    private static final String TEST_LONGEST_NAME = "longestName";

    public static void main(String[] args) {
        if (args.length != 3) {
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
            default:
                System.out.println("ERROR: Test name not recognized.");
                printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("Analyzing graph data with Halyard");
        System.out.println("Usage: $java -cp twitter-timeline-1.0-jar-with-dependencies.jar cz.vutbr.fit.xtutko00.MainHalyard [server_url] [repository_name] [test_name]");
        System.out.println("Tests:");
        System.out.println(TEST_LONGEST_NAME + " - get entry with the longest text");
    }
}
