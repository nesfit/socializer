package cz.vutbr.fit.xtutko00.model.rdf;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.T;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;
import cz.vutbr.fit.xtutko00.model.rdf.vocabulary.TA;
import cz.vutbr.fit.xtutko00.utils.IdMaker;

/**
 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#GeoContent>}
 */
public class GeoContent extends Content
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#GeoContent");

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#latitude>}
	 */
	private Double latitude;

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#longitude>}
	 */
	private Double longitude;


	public GeoContent(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return GeoContent.CLASS_IRI;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
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
	protected Object[] getProperties(IdMaker idMaker) {
		List<Object> properties = new ArrayList<>();

		properties.add(T.id);
		properties.add(idMaker.getId());
		properties.add(T.label);
		properties.add(buildLabel(getClassIRI()));

		if (getLabel() != null) {
			properties.add("label");
			properties.add(getLabel());
		}
		if (getLatitude() != null) {
			properties.add("latitude");
			properties.add(getLatitude());
		}
		if (getLongitude() != null) {
			properties.add("longitude");
			properties.add(getLongitude());
		}
		return properties.toArray();
	}
}
