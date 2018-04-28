/**
 * TwitterTimeline.java
 *
 * Created on 28. 7. 2017, 13:26:02 by burgetr
 */
package cz.vutbr.fit.xtutko00.source.twitter.model;

import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import org.eclipse.rdf4j.model.IRI;

/**
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author burgetr
 */
public class TwitterTimeline extends Timeline
{

    public TwitterTimeline(IRI iri)
    {
        super(iri);
    }

    @Override
    public String getLabel()
    {
        return "t_" + getSourceId();
    }
    
}
