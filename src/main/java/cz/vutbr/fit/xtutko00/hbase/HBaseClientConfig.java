package cz.vutbr.fit.xtutko00.hbase;

import java.io.Serializable;

public class HBaseClientConfig implements Serializable {

    private String tableName;
    private String hbaseSiteFilename;

    public String getTableName() {
        return tableName;
    }

    public HBaseClientConfig setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getHBaseSiteFilename() {
        return hbaseSiteFilename;
    }

    public HBaseClientConfig setHBaseSiteFilename(String hbaseSiteFilename) {
        this.hbaseSiteFilename = hbaseSiteFilename;
        return this;
    }
}
