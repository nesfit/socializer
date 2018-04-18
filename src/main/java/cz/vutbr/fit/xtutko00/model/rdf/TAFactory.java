package cz.vutbr.fit.xtutko00.model.rdf;

import org.eclipse.rdf4j.model.IRI;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;

public interface TAFactory extends EntityFactory {
	public GeoContent createGeoContent(IRI iri);
	public Content createContent(IRI iri);
	public TextContent createTextContent(IRI iri);
	public Image createImage(IRI iri);
	public Entry createEntry(IRI iri);
	public URLContent createURLContent(IRI iri);
	public Timeline createTimeline(IRI iri);
}