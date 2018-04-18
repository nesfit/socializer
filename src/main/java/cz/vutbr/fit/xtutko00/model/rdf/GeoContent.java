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
 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#GeoContent>}
 */
public class GeoContent extends Content
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#GeoContent");

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#latitude>}
	 */
	private double latitude;

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#longitude>}
	 */
	private double longitude;


	public GeoContent(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return GeoContent.CLASS_IRI;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
		addValue(model, TA.latitude, latitude);
		addValue(model, TA.longitude, longitude);
	}

	@Override
	public void loadFromModel(Model model, EntityFactory efactory) {
		super.loadFromModel(model, efactory);
		final Model m = model.filter(getIRI(), null, null);
		latitude = loadDoubleValue(m, TA.latitude);
		longitude = loadDoubleValue(m, TA.longitude);
	}

	@Override
	public Vertex addToGraph(HBaseGraph graph, IdMaker idMaker) {
		Vertex vertex = graph.addVertex(T.id, idMaker.getId(), T.label, buildLabel(getClassIRI()));
		addProperty(vertex, "label", getLabel());
		addProperty(vertex, "latitude", getLatitude());
		addProperty(vertex, "longitude", getLongitude());
		return vertex;
	}
}
