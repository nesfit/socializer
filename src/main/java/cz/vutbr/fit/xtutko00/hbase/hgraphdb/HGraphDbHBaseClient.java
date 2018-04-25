package cz.vutbr.fit.xtutko00.hbase.hgraphdb;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import cz.vutbr.fit.xtutko00.hbase.HBaseClient;
import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import cz.vutbr.fit.xtutko00.utils.Logger;
import cz.vutbr.fit.xtutko00.utils.XmlResourceParser;
import io.hgraphdb.HBaseBulkLoader;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;

public class HGraphDbHBaseClient implements HBaseClient {

    private final Logger logger = new Logger(HGraphDbHBaseClient.class);

    private HBaseClientConfig config;

    public HGraphDbHBaseClient(HBaseClientConfig config) {
        this.config = config;
    }

    @Override
    public void saveTimeline(Timeline timeline) {
        logger.info("START: Saving timeline of " + timeline.getSourceId());

        if (!isConfigOk()) {
            logger.error("Wrong configuration!");
            return;
        }

        logger.info("Opening HBaseGraph session.");
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration(false));
        HBaseBulkLoader loader = new HBaseBulkLoader(graph);
        IdMaker idMaker = new IdMaker(timeline.getLabel());

        timeline.addToGraph(loader, idMaker);

        loader.close();
        graph.close();

        logger.info("END: Saving timeline of " + timeline.getSourceId());
    }

    public void createTable() {
        if (!isConfigOk()) {
            logger.error("Cannot create table. Wrong configuration.");
            return;
        }

        logger.info("Creating HBase table " + getTableName());
        ((HBaseGraph) GraphFactory.open(getHBaseConfiguration(true))).close();
    }

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

    private String getTableName() {
        return config.getTableName() + "_hgraphdb";
    }

}
