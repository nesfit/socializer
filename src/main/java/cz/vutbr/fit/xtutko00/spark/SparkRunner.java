package cz.vutbr.fit.xtutko00.spark;

import cz.vutbr.fit.xtutko00.hbase.HBaseClient;
import cz.vutbr.fit.xtutko00.hbase.HBaseClientFactory;
import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.source.Source;
import cz.vutbr.fit.xtutko00.source.SourceClient;
import cz.vutbr.fit.xtutko00.source.SourceClientFactory;
import cz.vutbr.fit.xtutko00.utils.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.sql.Time;
import java.util.List;

/**
 * Runs Spark jobs.
 *
 * @author xtutko00
 */
public class SparkRunner implements Serializable {

    private final Logger logger = new Logger(SparkRunner.class);

    private SparkRunnerConf conf;

    public SparkRunner(SparkRunnerConf conf) {
        this.conf = conf;
    }

    /**
     * Downloads timelines from sources on Spark cluster and saves them into HBase table.
     */
    public void downloadTimelines() {
        if (!isConfigurationOk()) {
            logger.error("Wrong configuration.");
            return;
        }

        logger.info("Creating HBase table.");
        conf.getHBaseClientFactory().createClient().createTable();

        SourceClientFactory sourceClientFactory = conf.getSourceClientFactory();

        // Spark configuration
        SparkConf sparkConf = buildSparkConf();
        sparkConf.validateSettings();

        // Paralleling sources
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<Source> sourcesRDD = context.parallelize(conf.getSources(),conf.getNumOfPartitions());

        logger.info("START: Running spark job on " + sourcesRDD.getNumPartitions() + " partitions.");

        sourcesRDD.foreach(source -> {

            logger.info("START: Processing source: " + source.getType() + ":" + source.getName());

            // connecting to FB/Twitter
            SourceClient sourceClient = sourceClientFactory.createSourceClient(source.getType());
            if (sourceClient == null) {
                logger.error("Cannot create TimelineSource for source " + source.getName());
                return;
            }

            // downloading timeline
            Timeline timeline = sourceClient.getTimeline(source.getName());
            if (timeline == null) {
                logger.error("Could not download timeline of source " + source.getName());
                return;
            }

            logger.info("Timeline " + timeline.getLabel() + " has " + timeline.getEntries().size() + " posts");

            // saving timeline to HBase
            HBaseClient hBaseClient = this.conf.getHBaseClientFactory().createClient();
            hBaseClient.saveTimeline(timeline);

            logger.info("END: Processing source: " + source.getType() + ":" + source.getName());
        });

        logger.info("END: Running spark job.");
    }

    /**
     * Compares HBase clients writing speeds. Firstly all required timelines are downloaded from source, then
     * these timelines are saved once with Halyard and then with HGraphDB framework.
     */
    public void compareHBaseClients() {
        if (!isConfigurationOk()) {
            logger.error("Wrong configuration.");
            return;
        }

        logger.info("Creating HBase table.");
        conf.getHBaseClientFactory().createClient(HBaseClientFactory.EClient.HALYARD).createTable();
        conf.getHBaseClientFactory().createClient(HBaseClientFactory.EClient.HGRAPHDB).createTable();

        SourceClientFactory sourceClientFactory = conf.getSourceClientFactory();

        // Spark configuration
        SparkConf sparkConf = buildSparkConf();
        sparkConf.validateSettings();

        // Paralleling sources
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<Source> sourcesRDD = context.parallelize(conf.getSources(),conf.getNumOfPartitions());

        logger.info("START: Running spark job on " + sourcesRDD.getNumPartitions() + " partitions.");

        logger.info("START: Downloading timelines.");

        // Downloading Timelines
        JavaRDD<Timeline> timelinesRDD = sourcesRDD.map(source -> {

            logger.info("START: Processing source: " + source.getType() + ":" + source.getName());

            // connecting to FB/Twitter
            SourceClient sourceClient = sourceClientFactory.createSourceClient(source.getType());
            if (sourceClient == null) {
                logger.error("Cannot create TimelineSource for source " + source.getName());
                return null;
            }

            // downloading timeline
            Timeline timeline = sourceClient.getTimeline(source.getName());
            if (timeline == null) {
                logger.error("Could not download timeline of source " + source.getName());
                return null;
            }

            logger.info("END: Processing source: " + source.getType() + ":" + source.getName());

            return timeline;
        });

        logger.info("END: Downloading timelines. Number of timelines: " + timelinesRDD.count());

        logger.info("Start: Collecting timelines.");
        // Timelines have to be collected because we need to clear time of writing into DB
        List<Timeline> timelines = timelinesRDD.collect();
        logger.info("End: Collecting timelines. Num of timelines " + timelines.size());

        JavaRDD<Timeline> timelinesToSaveRDD = context.parallelize(timelines, conf.getNumOfPartitions());

        logger.info("START: Saving timelines with Halyard.");

        // Saving timelines with Halyard
        timelinesToSaveRDD.foreach(timeline -> {
            if (timeline == null) {
                logger.warning("Received null timeline, probably some timeline could not be downloaded.");
                return;
            }

            logger.info("START: Saving timeline: " + timeline.getLabel() + " into Halyard.");

            logger.info("Timeline " + timeline.getLabel() + " has " + timeline.getEntries().size() + " posts");

            // saving timeline to HBase
            HBaseClient hBaseClient = this.conf.getHBaseClientFactory().createClient(HBaseClientFactory.EClient.HALYARD);
            hBaseClient.saveTimeline(timeline);

            logger.info("END: Saving timeline: " + timeline.getLabel() + " into Halyard.");
        });

        logger.info("END: Saving timelines with Halyard.");

        logger.info("START: Saving timelines with HGraphDB.");

        // Saving timelines with HGraphDB
        timelinesToSaveRDD.foreach(timeline -> {
            if (timeline == null) {
                logger.warning("Received null timeline, probably some timeline could not be downloaded.");
                return;
            }

            logger.info("START: Saving timeline: " + timeline.getLabel() + " into HGraphDB.");

            // saving timeline to HBase
            HBaseClient hBaseClient = this.conf.getHBaseClientFactory().createClient(HBaseClientFactory.EClient.HGRAPHDB);
            hBaseClient.saveTimeline(timeline);

            logger.info("END: Saving timeline: " + timeline.getLabel() + " into HGraphDB.");
        });

        logger.info("END: Saving timelines with HGraphDB.");

        logger.info("END: Running spark job.");
    }

    private boolean isConfigurationOk() {
        if (this.conf.getHBaseClientFactory() == null) {
            return false;
        }

        if (this.conf.getSourceClientFactory() == null) {
            return false;
        }

        if (this.conf.getNumOfPartitions() == null || this.conf.getNumOfPartitions() <= 0) {
            return false;
        }

        return true;
    }

    private SparkConf buildSparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName(SparkRunner.class.getName());
        conf.getSparkProperties().forEach(sparkConf::set);
        return sparkConf;
    }
}
