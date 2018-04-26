package cz.vutbr.fit.xtutko00.hbase.halyard;

import com.msd.gin.halyard.common.HalyardTableUtils;
import cz.vutbr.fit.xtutko00.hbase.HBaseClient;
import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.utils.Logger;
import cz.vutbr.fit.xtutko00.utils.StopWatch;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class HalyardHBaseClient implements HBaseClient {

    private final Logger logger = new Logger(HalyardHBaseClient.class);

    private static final int DEFAULT_SPLIT_BITS = 3;

    private HBaseClientConfig config;

    public HalyardHBaseClient() {}

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

        logger.debug("START: Saving keyValues into \"" + getTableName() + "\" table.");
        Configuration conf = HBaseConfiguration.create();
        try (HTable hTable = HalyardTableUtils.getTable(conf, getTableName(), false, DEFAULT_SPLIT_BITS)) {
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
        logger.debug("END: Saving keyValues into \"" + getTableName() + "\" table.");

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
            logger.info("Creating HBase table " + getTableName());
            HalyardTableUtils.getTable(conf, getTableName(), true, DEFAULT_SPLIT_BITS);
        } catch (IOException e) {
            logger.error("Cannot create HBase table: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void testLongestEntryText(String serverUrl, String repositoryName) {
        try (RepositoryConnection con = createDbConnection(serverUrl, repositoryName)) {
            String queryString = "select distinct ?entry ?sourceId ?textlen where { " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#sourceId> ?sourceId . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#contains> ?content . " +
                    "  ?content <http://nesfit.github.io/ontology/ta.owl#text> ?text .\n" +
                    "  BIND (strlen(?text) AS ?textlen) " +
                    "} " +
                    "order by desc (strlen(str(?text))) " +
                    "limit 1";

            StopWatch stopWatch = new StopWatch();

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            stopWatch.start();
            try (TupleQueryResult result = tupleQuery.evaluate()) {
                while (result.hasNext()) {  // iterate over the result
                    BindingSet bindingSet = result.next();
                    Value entry = bindingSet.getValue("entry");
                    Value sourceId = bindingSet.getValue("sourceId");
                    Value textLen = bindingSet.getValue("textlen");

                    System.out.println("Result:");
                    System.out.println("entry: " + entry.stringValue());
                    System.out.println("sourceId: " + sourceId.stringValue());
                    System.out.println("textLen: " + textLen.stringValue());
                }
            }
            stopWatch.stop();

            logger.info("Query evaluated in " + stopWatch.getTimeSec() + " seconds.");
        }
    }

    public void testEntryTimestamps(String serverUrl, String repositoryName) {
        try (RepositoryConnection con = createDbConnection(serverUrl, repositoryName)) {
            String queryString = "SELECT ?label ?sourceId ?timestamp " +
                    "WHERE { " +
                    "  ?timeline <http://www.w3.org/2000/01/rdf-schema#label> ?label . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#sourceTimeline> ?timeline . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#timestamp> ?timestamp . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#sourceId> ?sourceId " +
                    "  FILTER ( ?timestamp >= xsd:dateTime('2018-01-01T00:00:00.00Z') ) " +
                    "}";

            StopWatch stopWatch = new StopWatch();

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            stopWatch.start();
            try (TupleQueryResult result = tupleQuery.evaluate()) {
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    Value sourceId = bindingSet.getValue("sourceId");
                    Value timestamp = bindingSet.getValue("timestamp");
                    Value label = bindingSet.getValue("label");

                    System.out.println("Timeline label: " + label.stringValue() + " Entry sourceId: " + sourceId.stringValue() + " timestamp: " + timestamp.stringValue());
                }
            }
            stopWatch.stop();

            logger.info("Query evaluated in " + stopWatch.getTimeMillis() + " milliseconds.");
        }
    }

    public void testEntryTimestampsWithSort(String serverUrl, String repositoryName) {
        try (RepositoryConnection con = createDbConnection(serverUrl, repositoryName)) {
            String queryString = "SELECT ?label ?sourceId ?timestamp " +
                    "WHERE { " +
                    "  ?timeline <http://www.w3.org/2000/01/rdf-schema#label> ?label . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#sourceTimeline> ?timeline . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#timestamp> ?timestamp . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#sourceId> ?sourceId " +
                    "  FILTER ( ?timestamp >= xsd:dateTime('2018-01-01T00:00:00.00Z') ) " +
                    "} ORDER BY DESC(?timestamp)";

            StopWatch stopWatch = new StopWatch();

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            stopWatch.start();
            try (TupleQueryResult result = tupleQuery.evaluate()) {
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    Value sourceId = bindingSet.getValue("sourceId");
                    Value timestamp = bindingSet.getValue("timestamp");
                    Value label = bindingSet.getValue("label");

                    System.out.println("Timeline label: " + label.stringValue() + " Entry sourceId: " + sourceId.stringValue() + " timestamp: " + timestamp.stringValue());
                }
            }
            stopWatch.stop();

            logger.info("Query evaluated in " + stopWatch.getTimeMillis() + " milliseconds.");
        }
    }

    public void testNumberOfEntries(String serverUrl, String repositoryName) {
        try (RepositoryConnection con = createDbConnection(serverUrl, repositoryName)) {
            String queryString = "SELECT ?label (COUNT(?entry) as ?numberOfEntries) " +
                    "WHERE{ " +
                    "  ?timeline <http://www.w3.org/2000/01/rdf-schema#label> ?label . " +
                    "  ?entry <http://nesfit.github.io/ontology/ta.owl#sourceTimeline> ?timeline . " +
                    "} GROUP BY ?label";

            StopWatch stopWatch = new StopWatch();

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            stopWatch.start();
            try (TupleQueryResult result = tupleQuery.evaluate()) {
                while (result.hasNext()) {
                    BindingSet bindingSet = result.next();
                    Value label = bindingSet.getValue("label");
                    Value numberOfEntries = bindingSet.getValue("numberOfEntries");

                    System.out.println("Timeline label: " + label.stringValue() + " count: " + numberOfEntries.stringValue());
                }
            }
            stopWatch.stop();

            logger.info("Query evaluated in " + stopWatch.getTimeMillis() + " milliseconds.");
        }
    }

    private RepositoryConnection createDbConnection(String serverUrl, String repositoryName) {
        Repository repo = new HTTPRepository(serverUrl, repositoryName);
        repo.initialize();
        return repo.getConnection();
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

    private String getTableName() {
        return config.getTableName() + "_halyard";
    }
}
