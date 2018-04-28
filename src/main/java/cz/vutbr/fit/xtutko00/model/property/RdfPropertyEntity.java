package cz.vutbr.fit.xtutko00.model.property;

import java.util.Collections;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;

import cz.vutbr.fit.xtutko00.model.core.RDFEntity;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import io.hgraphdb.HBaseBulkLoader;

/**
 * Wrapper for RDF entity so it can be saved into Property Graph Model.
 *
 * @author xtutko00
 */
public abstract class RdfPropertyEntity extends RDFEntity {

    public RdfPropertyEntity(IRI iri) {
        super(iri);
    }

    protected abstract Object[] getProperties(IdMaker idMaker);

    /**
     * Insert entity into graph.
     */
    public Vertex addToGraph(HBaseBulkLoader loader, IdMaker idMaker) {
        Vertex vertex = loader.addVertex(getProperties(idMaker));

        getEntities().forEach(entry -> {
            Vertex entryVertex = entry.addToGraph(loader, idMaker);
            loader.addEdge(vertex, entryVertex, "has", T.id, idMaker.getId());
        });

        return vertex;
    }

    protected Set<RdfPropertyEntity> getEntities() {
        return Collections.emptySet();
    }

    protected String buildLabel(IRI iri) {
        return iri.getNamespace() + iri.getLocalName();
    }
}
