/**
 * FBTimeline.java
 *
 * Created on 7. 3. 2018, 13:08:35 by burgetr
 */
package cz.vutbr.fit.xtutko00.source.facebook.model;

import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import org.eclipse.rdf4j.model.IRI;

/**
 * 
 * @author burgetr
 */
public class FBTimeline extends Timeline
{

    public FBTimeline(IRI iri)
    {
        super(iri);
    }
    
    @Override
    public String getLabel()
    {
        return "f_" + getSourceId();
    }

}
