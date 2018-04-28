package cz.vutbr.fit.xtutko00.model.core;


import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author burgetr
 */
public class ResourceFactory
{
    private final static ValueFactory vf = SimpleValueFactory.getInstance();
    private final static String RESOURCE_PREFIX = "http://nesfit.github.io/resource/ta#";

    public static IRI createResourceIRI(String module, String type, String id)
    {
        return vf.createIRI(RESOURCE_PREFIX, module + "-" + type + "-" + id);
    }

}
