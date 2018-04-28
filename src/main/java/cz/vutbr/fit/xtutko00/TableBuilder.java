package cz.vutbr.fit.xtutko00;

import java.util.Objects;

import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.hbase.halyard.HalyardHBaseClient;
import cz.vutbr.fit.xtutko00.hbase.hgraphdb.HGraphDbHBaseClient;
import cz.vutbr.fit.xtutko00.utils.Logger;

/**
 * Utility for creating HBase tables.
 *
 * @author xtutko00
 */
public class TableBuilder {

    private static final Logger logger = new Logger(MainSpark.class);

    private static final String HBASE_SITE_FILENAME = "/hbase-site.xml";

    /**
     * Main method.
     */
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("No args. Usage: $ [halyard/hgraphdb] [table_name]");
            return;
        }

        if (!Objects.equals(args[0], "halyard") && !Objects.equals(args[0], "hgraphdb")) {
            logger.error("First argument has to be halyard or hgraphdb.");
            return;
        }

        HBaseClientConfig hBaseClientConfig = new HBaseClientConfig()
                .setTableName(args[1])
                .setHBaseSiteFilename(HBASE_SITE_FILENAME);

        switch (args[0]) {
            case "halyard":
                HalyardHBaseClient halyardHBaseClient = new HalyardHBaseClient(hBaseClientConfig);
                halyardHBaseClient.createTable();
                break;
            case "hgraphdb":
                HGraphDbHBaseClient hGraphDbHBaseClient = new HGraphDbHBaseClient(hBaseClientConfig);
                hGraphDbHBaseClient.createTable();
                break;
        }
    }
}
