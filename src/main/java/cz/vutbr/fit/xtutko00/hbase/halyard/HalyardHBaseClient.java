package cz.vutbr.fit.xtutko00.hbase.halyard;

import com.msd.gin.halyard.common.HalyardTableUtils;
import cz.vutbr.fit.xtutko00.hbase.HBaseClient;
import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.utils.Logger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class HalyardHBaseClient implements HBaseClient {

    private final Logger logger = new Logger(HalyardHBaseClient.class);

    private static final int DEFAULT_SPLIT_BITS = 3;

    private HBaseClientConfig config;

    public HalyardHBaseClient(HBaseClientConfig config) {
        this.config = config;
    }

    @Override
    public void saveTimeline(Timeline timeline) {
        logger.info("START: Saving timeline of " + timeline.getSourceId());

        if (!isConfigOk()) {
            logger.error("Wrong configuration!");
            return;
        }

        logger.debug("START: Parsing statements.");
        Set<Statement> statements = getStatements(timeline);
        if (CollectionUtils.isEmpty(statements)) {
            logger.error("Cannot parse timeline statements.");
            return;
        }
        logger.debug("END: Parsing statements. Parsed: " + statements.size() + " statements.");

        logger.debug("START: Parsing keyValues.");
        Set<KeyValue> keyValues = getKeyValues(statements);
        if (CollectionUtils.isEmpty(keyValues)) {
            logger.error("Cannot parse keyValues.");
            return;
        }
        logger.debug("END: Parsing keyValues. Parsed " + keyValues.size() + " keyValues.");

        logger.debug("START: Saving keyValues into \"" + this.config.getTableName() + "\" table.");
        Configuration conf = HBaseConfiguration.create();
        try (HTable hTable = HalyardTableUtils.getTable(conf, this.config.getTableName(), false, DEFAULT_SPLIT_BITS)) {
            keyValues.forEach(kv -> {
                try {
                    hTable.put(new Put(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), kv.getTimestamp()).add(kv));
                } catch (IOException e) {
                    logger.error("Cannot put keyValue into table.");
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            logger.error("Cannot save data into HBase: " + e.getMessage());
        }
        logger.debug("END: Saving keyValues into \"" + this.config.getTableName() + "\" table.");

        logger.info("END: Saving timeline of " + timeline.getSourceId());
    }

    @Override
    public void createTable() {
        if (!isConfigOk()) {
            logger.error("Cannot create HBase table. Wrong configuration.");
            return;
        }

        Configuration conf = HBaseConfiguration.create();
        try {
            logger.info("Creating HBase table " + config.getTableName());
            HalyardTableUtils.getTable(conf, this.config.getTableName(), true, DEFAULT_SPLIT_BITS);
        } catch (IOException e) {
            logger.error("Cannot create HBase table: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Set<Statement> getStatements(Timeline timeline) {
        Model model = new LinkedHashModel();
        timeline.addToModel(model);
        return model;
    }

    private Set<KeyValue> getKeyValues(Set<Statement> statements) {
        long timestamp = System.currentTimeMillis();

        Set<KeyValue> keyValues = new HashSet<>();
        statements.forEach(statement ->
                keyValues.addAll(
                        Arrays.asList(
                                HalyardTableUtils.toKeyValues(
                                        statement.getSubject(),
                                        statement.getPredicate(),
                                        statement.getObject(),
                                        null,
                                        false,
                                        timestamp
                                )
                        )
                )
        );

        return keyValues;
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
}
