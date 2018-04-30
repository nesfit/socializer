package cz.vutbr.fit.xtutko00.hbase.hgraphdb;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;


import cz.vutbr.fit.xtutko00.hbase.HBaseClient;
import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import cz.vutbr.fit.xtutko00.utils.Logger;
import cz.vutbr.fit.xtutko00.utils.StopWatch;
import cz.vutbr.fit.xtutko00.utils.XmlResourceParser;
import io.hgraphdb.HBaseBulkLoader;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;

/**
 * HGraphDb client.
 *
 * @author xtutko00
 */
public class HGraphDbHBaseClient implements HBaseClient {

    private final Logger logger = new Logger(HGraphDbHBaseClient.class);

    private HBaseClientConfig config;

    public HGraphDbHBaseClient(HBaseClientConfig config) {
        this.config = config;
    }

    /**
     * Saves given timeline into HBase table.
     */
    @Override
    public void saveTimeline(Timeline timeline) {
        logger.info("START: Saving timeline of " + timeline.getSourceId());

        if (!isConfigOk()) {
            logger.error("Wrong configuration!");
            return;
        }

        logger.info("Opening HBaseGraph session.");
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        HBaseBulkLoader loader = new HBaseBulkLoader(graph); // bulk loader speeds up writing
        IdMaker idMaker = new IdMaker(timeline.getLabel());

        timeline.addToGraph(loader, idMaker);

        loader.close();
        graph.close();

        logger.info("END: Saving timeline of " + timeline.getSourceId());
    }

    /**
     * Creates required HBase tables with "_hgraphdb" suffix.
     */
    public void createTable() {
        if (!isConfigOk()) {
            logger.error("Cannot create table. Wrong configuration.");
            return;
        }

        logger.info("Creating HBase table " + getTableName());
        ((HBaseGraph) GraphFactory.open(getHBaseConfiguration(true))).close();
    }

    /**
     * Prints entry with the longest text content.
     */
    public void testLongestEntryText() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        GraphTraversalSource g = graph.traversal();

        StopWatch stopWatchPrinted = new StopWatch();
        StopWatch stopWatchEvaluate = new StopWatch();

        stopWatchPrinted.start();
        stopWatchEvaluate.start();
        GraphTraversal<Vertex, Map<String, Object>> result = g.V()
                .hasLabel("http://nesfit.github.io/ontology/ta.owl#TextContent")
                .filter(in("has").hasLabel("http://nesfit.github.io/ontology/ta.owl#Entry"))
                .order().by(v -> {
                    if (((Vertex) v).property("text").isPresent()) {
                        return ((Vertex) v).<String>property("text").value().length();
                    } else {
                        return 0;
                    }
                }, Order.decr)
                .limit(1)
                .project("text", "sourceId")
                .by(values("text"))
                .by(in("has").values("sourceId"));

        while(result.hasNext()) {
            Map<String, Object> map = result.next();
            stopWatchEvaluate.stop();
            System.out.println("sourceId: " + map.get("sourceId"));
            System.out.println("textlen: " + ((String)map.get("text")).length());
        }
        stopWatchPrinted.stop();
        logger.info("Query evaluated in " + stopWatchEvaluate.getTimeMillis() + " milliseconds.");
        logger.info("Query printed in " + stopWatchPrinted.getTimeMillis() + " milliseconds.");

        graph.close();
    }

    /**
     * Prints entries newer than year 2018.
     */
    public void testEntryTimestamps() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        GraphTraversalSource g = graph.traversal();

        StopWatch stopWatchPrinted = new StopWatch();
        StopWatch stopWatchEvaluate = new StopWatch();

        stopWatchPrinted.start();
        stopWatchEvaluate.start();
        Date year2018 = new GregorianCalendar(2018, Calendar.JANUARY, 1).getTime();
        GraphTraversal<Vertex, Map<String, Object>> result = g.V()
                .hasLabel("http://nesfit.github.io/ontology/ta.owl#Entry")
                .filter(in("has"))
                .has("timestamp", P.gt(year2018))
                .project("sourceId", "timestamp", "label")
                .by(values("sourceId"))
                .by(values("timestamp"))
                .by(in("has").values("label"));

        while(result.hasNext()) {
            Map<String, Object> map = result.next();
            stopWatchEvaluate.stop();
            System.out.println("Timeline label: " + map.get("label") + " Entry sourceId: " + map.get("sourceId") + " timestamp: " + map.get("timestamp"));
        }
        stopWatchPrinted.stop();
        logger.info("Query evaluated in " + stopWatchEvaluate.getTimeMillis() + " milliseconds.");
        logger.info("Query printed in " + stopWatchPrinted.getTimeMillis() + " milliseconds.");

        graph.close();
    }

    /**
     * Prints entries newer than year 2018, sorted by creation time.
     */
    public void testEntryTimestampsWithSort() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        GraphTraversalSource g = graph.traversal();

        StopWatch stopWatchPrinted = new StopWatch();
        StopWatch stopWatchEvaluate = new StopWatch();

        stopWatchPrinted.start();
        stopWatchEvaluate.start();
        Date year2018 = new GregorianCalendar(2018, Calendar.JANUARY, 1).getTime();
        GraphTraversal<Vertex, Map<String, Object>> result = g.V()
                .hasLabel("http://nesfit.github.io/ontology/ta.owl#Entry")
                .filter(in("has"))
                .has("timestamp", P.gt(year2018))
                .order()
                .by(values("timestamp"), Order.decr)
                .project("sourceId", "timestamp", "label")
                .by(values("sourceId"))
                .by(values("timestamp"))
                .by(in("has").values("label"));

        while(result.hasNext()) {
            Map<String, Object> map = result.next();
            stopWatchEvaluate.stop();
            System.out.println("Timeline label: " + map.get("label") + " Entry sourceId: " + map.get("sourceId") + " timestamp: " + map.get("timestamp"));
        }
        stopWatchPrinted.stop();
        logger.info("Query evaluated in " + stopWatchEvaluate.getTimeMillis() + " milliseconds.");
        logger.info("Query printed in " + stopWatchPrinted.getTimeMillis() + " milliseconds.");

        graph.close();
    }

    /**
     * Prints timelines with theirs number of entries.
     */
    public void testNumberOfEntries() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        GraphTraversalSource g = graph.traversal();

        StopWatch stopWatchPrinted = new StopWatch();
        StopWatch stopWatchEvaluate = new StopWatch();

        stopWatchPrinted.start();
        stopWatchEvaluate.start();
        GraphTraversal<Vertex, Map<Object, Long>> result = g.V()
                .hasLabel("http://nesfit.github.io/ontology/ta.owl#Entry")
                .filter(in("has"))
                .groupCount()
                .by(in("has").values("label"));

        while(result.hasNext()) {
            Map<Object, Long> map = result.next();
            stopWatchEvaluate.stop();
            Map.Entry<Object,Long> entry = map.entrySet().iterator().next();
            System.out.println("Timeline label: " + entry.getKey() + " count: " + entry.getValue());
        }
        stopWatchPrinted.stop();
        logger.info("Query evaluated in " + stopWatchEvaluate.getTimeMillis() + " milliseconds.");
        logger.info("Query printed in " + stopWatchPrinted.getTimeMillis() + " milliseconds.");

        graph.close();
    }

    /**
     * Prints number of occurrences of urls in posts, sorted by number of occurrences.
     */
    public void testSharedUrls() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        GraphTraversalSource g = graph.traversal();

        StopWatch stopWatchPrinted = new StopWatch();
        StopWatch stopWatchEvaluate = new StopWatch();

        stopWatchPrinted.start();
        stopWatchEvaluate.start();
        GraphTraversal<Vertex, Object> result = g.V()
                .hasLabel("http://nesfit.github.io/ontology/ta.owl#URLContent")
                .groupCount().by(values("sourceUrl").as("count"))
                .unfold()
                .filter(entry -> ((Map.Entry<Object, Long>) entry.get()).getValue() > 1)
                .order().by(entry -> ((Map.Entry<Object, Long>) entry).getValue(), Order.decr);

        while(result.hasNext()) {
            stopWatchEvaluate.stop();
            Map.Entry<Object, Long> entry = (Map.Entry<Object, Long>) result.next();
            System.out.println("SourceUrl: " + entry.getKey() + " count: " + entry.getValue());
        }
        stopWatchPrinted.stop();
        logger.info("Query evaluated in " + stopWatchEvaluate.getTimeMillis() + " milliseconds.");
        logger.info("Query printed in " + stopWatchPrinted.getTimeMillis() + " milliseconds.");

        graph.close();
    }

    /**
     * Checks configuration.
     *
     * @return true if configuration is ok
     */
    private boolean isConfigOk() {
        if (this.config == null) {
            return false;
        }

        if (this.config.getHBaseSiteFilename() == null) {
            return false;
        }

        if (this.config.getTableName() == null || this.config.getTableName().length() == 0) {
            return false;
        }

        return true;
    }

    /**
     * Creates HGraphDB configuration, based on table name and HBase config.
     */
    private Configuration getHBaseConfiguration(boolean createTable) {
        HBaseGraphConfiguration cfg = new HBaseGraphConfiguration()
                .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace(getTableName())
                .setCreateTables(createTable);

        XmlResourceParser hBaseConfigParser = new XmlResourceParser(this.config.getHBaseSiteFilename());
        Map<String, String> keyValues = hBaseConfigParser.parse();
        keyValues.forEach(cfg::set);

        return cfg;
    }

    /**
     * Adds "_hgraphdb" suffix to table name.
     */
    private String getTableName() {
        return config.getTableName() + "_hgraphdb";
    }

}
