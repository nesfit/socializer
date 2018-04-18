package cz.vutbr.fit.xtutko00.model.rdf;

import com.github.radkovo.rdf4j.builder.EntityFactory;
import cz.vutbr.fit.xtutko00.model.property.RdfPropertyEntity;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import io.hgraphdb.HBaseGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

/**
 * A particular content of an entry..
 * <p>
 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#Content>}
 */
public class Content extends RdfPropertyEntity
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#Content");


	public Content(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return Content.CLASS_IRI;
	}

	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
	}

	@Override
	public void loadFromModel(Model model, EntityFactory efactory) {
		super.loadFromModel(model, efactory);
		final Model m = model.filter(getIRI(), null, null);
	}

	@Override
	public Vertex addToGraph(HBaseGraph graph, IdMaker idMaker) {
		Vertex vertex = graph.addVertex(T.id, idMaker.getId(), T.label, buildLabel(getClassIRI()));
		addProperty(vertex, "label", getLabel());
		return vertex;
	}
}
