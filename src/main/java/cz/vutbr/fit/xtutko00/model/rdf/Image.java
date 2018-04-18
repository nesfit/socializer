package cz.vutbr.fit.xtutko00.model.rdf;

import com.github.radkovo.rdf4j.builder.EntityFactory;
import cz.vutbr.fit.xtutko00.model.rdf.vocabulary.TA;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import io.hgraphdb.HBaseGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

/**
 * An image contained in an entry..
 * <p>
 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#Image>}
 */
public class Image extends Content
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#Image");

	/**
	 * Source URL of the media..
	 * <p>
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#sourceUrl>}
	 */
	private String sourceUrl;


	public Image(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return Image.CLASS_IRI;
	}

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
		addValue(model, TA.sourceUrl, sourceUrl);
	}

	@Override
	public void loadFromModel(Model model, EntityFactory efactory) {
		super.loadFromModel(model, efactory);
		final Model m = model.filter(getIRI(), null, null);
		sourceUrl = loadStringValue(m, TA.sourceUrl);
	}

	@Override
	public Vertex addToGraph(HBaseGraph graph, IdMaker idMaker) {
		Vertex vertex = graph.addVertex(T.id, idMaker.getId(), T.label, buildLabel(getClassIRI()));
		addProperty(vertex, "label", getLabel());
		addProperty(vertex, "sourceUrl", getSourceUrl());
		return vertex;
	}
}
