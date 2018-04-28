/**
 * FBEntry.java
 *
 * Created on 7. 3. 2018, 13:09:41 by burgetr
 */
package cz.vutbr.fit.xtutko00.source.facebook.model;

import cz.vutbr.fit.xtutko00.model.rdf.Entry;
import org.eclipse.rdf4j.model.IRI;

/**
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author burgetr
 */
public class FBEntry extends Entry
{

    public FBEntry(IRI iri)
    {
        super(iri);
    }

}
