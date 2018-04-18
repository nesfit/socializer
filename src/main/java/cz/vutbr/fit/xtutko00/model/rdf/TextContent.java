package cz.vutbr.fit.xtutko00.model.rdf;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;
import cz.vutbr.fit.xtutko00.model.rdf.vocabulary.TA;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import io.hgraphdb.HBaseGraph;

/**
 * Text contained in an entry..
 * <p>
 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#TextContent>}
 */
public class TextContent extends Content
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#TextContent");

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#text>}
	 */
	private String text;


	public TextContent(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return TextContent.CLASS_IRI;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
		addValue(model, TA.text, text);
	}

	@Override
	public void loadFromModel(Model model, EntityFactory efactory) {
		super.loadFromModel(model, efactory);
		final Model m = model.filter(getIRI(), null, null);
		text = loadStringValue(m, TA.text);
	}

	@Override
	public Vertex addToGraph(HBaseGraph graph, IdMaker idMaker) {
		Vertex vertex = graph.addVertex(T.id, idMaker.getId(), T.label, buildLabel(getClassIRI()));
		addProperty(vertex, "label", getLabel());
		addProperty(vertex, "text", getText());
		return vertex;
	}
}
