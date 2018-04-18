package cz.vutbr.fit.xtutko00.spark;

import cz.vutbr.fit.xtutko00.hbase.HBaseClientFactory;
import cz.vutbr.fit.xtutko00.source.Source;
import cz.vutbr.fit.xtutko00.source.SourceClientFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration for SparkRunner class.
 *
 * @author xtutko00
 */
public class SparkRunnerConf implements Serializable {

    private List<Source> sources;
    private SourceClientFactory sourceClientFactory;
    private HBaseClientFactory hBaseClientFactory;
    private Map<String, String> sparkProperties;
    private Integer numOfPartitions;

    public List<Source> getSources() {
        if (sources == null) {
            return new ArrayList<>();
        }

        return sources;
    }

    public SparkRunnerConf setSources(List<Source> sources) {
        this.sources = sources;
        return this;
    }

    public SourceClientFactory getSourceClientFactory() {
        return sourceClientFactory;
    }

    public SparkRunnerConf setSourceClientFactory(SourceClientFactory sourceClientFactory) {
        this.sourceClientFactory = sourceClientFactory;
        return this;
    }

    public HBaseClientFactory getHBaseClientFactory() {
        return hBaseClientFactory;
    }

    public SparkRunnerConf setHBaseClientFactory(HBaseClientFactory hBaseClientFactory) {
        this.hBaseClientFactory = hBaseClientFactory;
        return this;
    }

    public Map<String, String> getSparkProperties() {
        if (this.sparkProperties == null) {
            this.sparkProperties = new HashMap<>();
        }

        return this.sparkProperties;
    }

    public Integer getNumOfPartitions() {
        return numOfPartitions;
    }

    public SparkRunnerConf setNumOfPartitions(Integer numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
        return this;
    }

    public void addSparkProperty(String key, String value) {
        getSparkProperties().put(key, value);
    }
}
