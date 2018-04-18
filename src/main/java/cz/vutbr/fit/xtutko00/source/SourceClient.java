package cz.vutbr.fit.xtutko00.source;

import cz.vutbr.fit.xtutko00.model.rdf.Timeline;

import java.io.Serializable;

public interface SourceClient extends Serializable {
    Timeline getTimeline(String sourceName);
}
