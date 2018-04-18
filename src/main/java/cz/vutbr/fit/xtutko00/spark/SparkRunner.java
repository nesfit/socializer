package cz.vutbr.fit.xtutko00.spark;

import cz.vutbr.fit.xtutko00.hbase.HBaseClient;
import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.source.Source;
import cz.vutbr.fit.xtutko00.source.SourceClient;
import cz.vutbr.fit.xtutko00.source.SourceClientFactory;
import cz.vutbr.fit.xtutko00.utils.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

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
     * Downloads timelines from sources on Spark cluster
     */
    public void downloadTimelines() {
        if (!isConfigurationOk()) {
            logger.error("Wrong configuration.");
            return;
        }

        logger.info("Creating HBase table.");
        conf.getHBaseClientFactory().getInstance().createTable();

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

            // saving timeline to HBase
            HBaseClient hBaseClient = this.conf.getHBaseClientFactory().getInstance();
            hBaseClient.saveTimeline(timeline);

            logger.info("END: Processing source: " + source.getType() + ":" + source.getName());
        });

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
