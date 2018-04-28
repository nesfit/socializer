package cz.vutbr.fit.xtutko00.model.rdf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;
import cz.vutbr.fit.xtutko00.model.property.RdfPropertyEntity;
import cz.vutbr.fit.xtutko00.model.rdf.vocabulary.TA;
import cz.vutbr.fit.xtutko00.utils.IdMaker;
import io.hgraphdb.HBaseBulkLoader;

/**
 * Entry.
 *
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author xtutko00
 */
public class Entry extends RdfPropertyEntity
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#Entry");

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#sourceTimeline>}
	 */
	private Timeline sourceTimeline;

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#contains>}
	 */
	private Set<Content> contains;

	/**
	 * An identifier of the entry in the source timeline (e.g. Twitter id).
	 * <p>
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#sourceId>}
	 */
	private String sourceId;

	/**
	 * Entry creation timestamp.
	 * <p>
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#timestamp>}
	 */
	private java.util.Date timestamp;


	public Entry(IRI iri) {
		super(iri);
		contains = new HashSet<>();
	}

	@Override
	public IRI getClassIRI() {
		return Entry.CLASS_IRI;
	}

	public Timeline getSourceTimeline() {
		return sourceTimeline;
	}

	public void setSourceTimeline(Timeline sourceTimeline) {
		this.sourceTimeline = sourceTimeline;
	}

	public Set<Content> getContains() {
		return contains;
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public java.util.Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(java.util.Date timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
	 *
	 * @author burgetr
	 */
	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
		addObject(model, TA.sourceTimeline, sourceTimeline);
		addCollectionWithData(model, TA.contains, contains);
		addValue(model, TA.sourceId, sourceId);
		addValue(model, TA.timestamp, timestamp);
	}

	/**
	 * Returns Entry properties.
	 */
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
		if (getSourceId() != null) {
			properties.add("sourceId");
			properties.add(getSourceId());
		}
		if (getTimestamp() != null) {
			properties.add("timestamp");
			properties.add(getTimestamp());
		}
		return properties.toArray();
	}

	/**
	 * Returns Entry content.
	 */
	@Override
	protected Set<RdfPropertyEntity> getEntities() {
		return Collections.unmodifiableSet(getContains());
	}
}
