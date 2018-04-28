package cz.vutbr.fit.xtutko00.model.rdf;

import org.eclipse.rdf4j.model.IRI;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;

/**
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author burgetr
 */
public interface TAFactory extends EntityFactory {
	GeoContent createGeoContent(IRI iri);
	Content createContent(IRI iri);
	TextContent createTextContent(IRI iri);
	Image createImage(IRI iri);
	Entry createEntry(IRI iri);
	URLContent createURLContent(IRI iri);
	Timeline createTimeline(IRI iri);
}
