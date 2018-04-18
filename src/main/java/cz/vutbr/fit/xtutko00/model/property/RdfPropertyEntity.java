package cz.vutbr.fit.xtutko00.model.property;

import cz.vutbr.fit.xtutko00.model.core.RDFEntity;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import io.hgraphdb.HBaseGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;

public abstract class RdfPropertyEntity extends RDFEntity {

    public RdfPropertyEntity(IRI iri) {
        super(iri);
    }

    public abstract Vertex addToGraph(HBaseGraph graph, IdMaker idMaker);

    protected <V> void addProperty(Vertex vertex, String key, V value) {
        if (key == null || value == null) {
            return;
        }
        vertex.property(key, value);
    }

    protected String buildLabel(IRI iri) {
        return iri.getNamespace() + iri.getLocalName();
    }
}
