package cz.vutbr.fit.xtutko00;

import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.hbase.HBaseClientFactory;
import cz.vutbr.fit.xtutko00.source.ESourceType;
import cz.vutbr.fit.xtutko00.source.Source;
import cz.vutbr.fit.xtutko00.source.SourceClientFactory;
import cz.vutbr.fit.xtutko00.source.facebook.FacebookSourceClientConfig;
import cz.vutbr.fit.xtutko00.source.twitter.TwitterSourceClientConfig;
import cz.vutbr.fit.xtutko00.spark.SparkRunner;
import cz.vutbr.fit.xtutko00.spark.SparkRunnerConf;
import cz.vutbr.fit.xtutko00.utils.Logger;
import cz.vutbr.fit.xtutko00.utils.XmlResourceParser;
import org.apache.commons.cli.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Entry point of the application.
 *
 * @author xtutko00
 */
public class MainSpark {

    private static final Logger logger = new Logger(MainSpark.class);

    private static final String FILENAME_HBASE_SITE = "/hbase-site.xml";
    private static final String FILENAME_CONFIGURATION = "/configuration.xml";

    private static final String RESOURCE_TWITTER_CONSUMER_KEY = "twitter.consumer.key";
    private static final String RESOURCE_TWITTER_CONSUMER_SECRET = "twitter.consumer.secret";
    private static final String RESOURCE_FACEBOOK_APP_ID = "facebook.app.id";
    private static final String RESOURCE_FACEBOOK_APP_SECRET = "facebook.app.secret";
    private static final String RESOURCE_FACEBOOK_UNTIL_DATE = "facebook.date.until";
    private static final String RESOURCE_NUM_OF_PARTITIONS = "cluster.partition_num";

    private static final String ARGUMENT_SOURCES = "s";
    private static final String ARGUMENT_HBASE_TABLENAME = "ht";
    private static final String ARGUMENT_HBASE_CLIENT = "hc";
    private static final String ARGUMENT_SOURCES_LONG = "sources";
    private static final String ARGUMENT_HBASE_TABLENAME_LONG = "hbase_tablename";
    private static final String ARGUMENT_HBASE_CLIENT_LONG = "hbase_client";

    /**
     * Main method.
     */
    public static void main(String[] args) {

        // Parsing command line arguments
        // --------------------------------------------------------
        CommandLine cmd = getCommandLineArguments(args);
        if(cmd == null) {
            logger.error("Cannot get command-line arguments.");
            return;
        }

        String twitterUsersFile = cmd.getOptionValue(ARGUMENT_SOURCES);
        if (StringUtils.isBlank(twitterUsersFile)) {
            logger.error("Twitter users file not set.");
            return;
        }

        String hBaseTableName = cmd.getOptionValue(ARGUMENT_HBASE_TABLENAME);
        if (StringUtils.isBlank(hBaseTableName)) {
            logger.error("HBase table name not set.");
            return;
        }

        String hBaseClient = cmd.getOptionValue(ARGUMENT_HBASE_CLIENT);
        if (StringUtils.isBlank(hBaseClient)) {
            logger.error("HBase client not set.");
            return;
        }

        // --------------------------------------------------------

        // Parsing FB and Twitter sources from input file.
        List<Source> sources = parseSourcesFromFile(twitterUsersFile);
        if (CollectionUtils.isEmpty(sources)) {
            logger.info("No sources to fetch given.");
            return;
        }

        // Parsing app configuration.
        Map<String, String> properties = new XmlResourceParser(FILENAME_CONFIGURATION).parse();

        // TimelineSources factory.
        TwitterSourceClientConfig twitterConfig = getTwitterConfig(properties);
        FacebookSourceClientConfig facebookConfig = getFacebookConfig(properties);
        SourceClientFactory sourceClientFactory = new SourceClientFactory(facebookConfig, twitterConfig);

        // HBase configuration.
        HBaseClientConfig hBaseClientConfig = new HBaseClientConfig()
                .setTableName(hBaseTableName)
                .setHBaseSiteFilename(FILENAME_HBASE_SITE);

        // HBaseClient factory.
        HBaseClientFactory hBaseClientFactory;
        switch (hBaseClient) {
            case "halyard":
                hBaseClientFactory = new HBaseClientFactory(HBaseClientFactory.EClient.HALYARD, hBaseClientConfig);
                break;
            case "hgraphdb":
                hBaseClientFactory = new HBaseClientFactory(HBaseClientFactory.EClient.HGRAPHDB, hBaseClientConfig);
                break;
            default:
                logger.error("HBase client not recognized. Please use halyard or hgraphdb.");
                return;
        }

        String numOfPartitionsStr = properties.get(RESOURCE_NUM_OF_PARTITIONS);
        if (StringUtils.isBlank(numOfPartitionsStr)) {
            logger.error("No number of partition given. Please set " + RESOURCE_NUM_OF_PARTITIONS + " param in configuration file.");
            return;
        }

        Integer numOfPartitions;
        try {
            numOfPartitions = Integer.parseInt(numOfPartitionsStr);
        } catch (NumberFormatException e) {
            logger.error("Cannot convert parameter " + RESOURCE_NUM_OF_PARTITIONS + " into long: " + e.getMessage());
            return;
        }

        if (numOfPartitions <= 0) {
            logger.error("Parameter " + RESOURCE_NUM_OF_PARTITIONS + " has to be greater than 0.");
            return;
        }

        // SparkRunner configuration.
        SparkRunnerConf conf = new SparkRunnerConf()
            .setSources(sources)
            .setSourceClientFactory(sourceClientFactory)
            .setHBaseClientFactory(hBaseClientFactory)
            .setNumOfPartitions(numOfPartitions);
        addSparkProperties(conf, properties);

        // Submitting Spark job.
        SparkRunner sparkRunner = new SparkRunner(conf);
        sparkRunner.downloadTimelines();
    }

    /**
     * Receive all TwitterSource config data from given properties.
     *
     * @return TwitterSource configuration or null when not all mandatory params were found
     */
    private static TwitterSourceClientConfig getTwitterConfig(Map<String, String> properties) {
        String consumerKey = properties.get(RESOURCE_TWITTER_CONSUMER_KEY);
        if (consumerKey == null) {
            logger.error("Please specify " + RESOURCE_TWITTER_CONSUMER_KEY + " in " + FILENAME_CONFIGURATION);
            return null;
        }

        String consumerSecret = properties.get(RESOURCE_TWITTER_CONSUMER_SECRET);
        if (consumerSecret == null) {
            logger.error("Please specify " + RESOURCE_TWITTER_CONSUMER_SECRET + " in " + FILENAME_CONFIGURATION);
            return null;
        }

        return new TwitterSourceClientConfig(consumerKey, consumerSecret);
    }

    /**
     * Receive all FacebookSource config data from given properties.
     *
     * @return FacebookSource configuration or null when not all mandatory params were found
     */
    private static FacebookSourceClientConfig getFacebookConfig(Map<String, String> properties) {
        String appId = properties.get(RESOURCE_FACEBOOK_APP_ID);
        if (appId == null) {
            logger.error("Please specify " + RESOURCE_FACEBOOK_APP_ID + " in " + FILENAME_CONFIGURATION);
            return null;
        }

        String appSecret = properties.get(RESOURCE_FACEBOOK_APP_SECRET);
        if (appSecret == null) {
            logger.error("Please specify " + RESOURCE_FACEBOOK_APP_SECRET + " in " + FILENAME_CONFIGURATION);
            return null;
        }

        String untilDate = properties.get(RESOURCE_FACEBOOK_UNTIL_DATE);

        return new FacebookSourceClientConfig(appId, appSecret, untilDate);
    }

    /**
     * Parse and checks all command line arguments. Prints help if not sufficient arguments were given.
     */
    private static CommandLine getCommandLineArguments(String[] args) {
        Options options = new Options();

        Option sourcesFile = new Option(ARGUMENT_SOURCES, ARGUMENT_SOURCES_LONG, true, "file with sources, each on new line (line format: T/F:sourceName)");
        sourcesFile.setRequired(true);
        options.addOption(sourcesFile);

        Option hBaseTableName = new Option(ARGUMENT_HBASE_TABLENAME, ARGUMENT_HBASE_TABLENAME_LONG, true, "name of the HBase table");
        hBaseTableName.setRequired(true);
        options.addOption(hBaseTableName);

        Option hBaseClient = new Option(ARGUMENT_HBASE_CLIENT, ARGUMENT_HBASE_CLIENT_LONG, true, "HBase client (halyard or hgraphdb)");
        hBaseClient.setRequired(true);
        options.addOption(hBaseClient);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Wrong input arguments.");
            formatter.printHelp("timeline-downloader", options);
            return null;
        }

        return cmd;
    }

    /**
     * Parses all Twitter/Facebook sources from given file.
     */
    private static List<Source> parseSourcesFromFile(String fileName) {
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            List<Source> sources = new ArrayList<>();
            stream.forEach(line -> {
                if (StringUtils.isNotBlank(line)) {
                    String firstChars = StringUtils.substring(line, 0, 2);
                    ESourceType sourceType;
                    switch (firstChars) {
                        case "F:":
                            sourceType = ESourceType.FACEBOOK;
                            break;
                        case "T:":
                            sourceType = ESourceType.TWITTER;
                            break;
                        default:
                            logger.error("Wrong line format (line should start with T: or F:): " + line);
                            return;
                    }

                    String sourceName = StringUtils.substring(line, 2);
                    if (StringUtils.isNotBlank(sourceName)) {
                        sources.add(new Source(sourceType, sourceName));
                    }
                }
            });
            return sources;
        } catch (IOException e) {
            logger.error("Cannot open file: " + fileName);
            return new ArrayList<>();
        }
    }

    /**
     * Fill up Spark configuration with all Spark related properties. Spark related properties are properties,
     * which start with "spark.".
     */
    private static void addSparkProperties(SparkRunnerConf conf, Map<String, String> properties) {
        properties.forEach((key, value) -> {
            if (key.startsWith("spark.")) {
                conf.addSparkProperty(key, value);
            }
        });
    }
}
