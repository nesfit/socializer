package cz.vutbr.fit.xtutko00.hbase;

import cz.vutbr.fit.xtutko00.model.rdf.Timeline;

import java.io.Serializable;

/**
 * HBase client.
 *
 * @author xtutko00
 */
public interface HBaseClient extends Serializable {
    void createTable();
    void saveTimeline(Timeline timeline);
}
