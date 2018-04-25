package cz.vutbr.fit.xtutko00.model.rdf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.T;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;
import cz.vutbr.fit.xtutko00.model.property.RdfPropertyEntity;
import cz.vutbr.fit.xtutko00.model.rdf.vocabulary.TA;
import cz.vutbr.fit.xtutko00.utils.IdMaker;

/**
 * A sequence of entries displayed in a signle time line..
 * <p>
 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#Timeline>}
 */
public class Timeline extends RdfPropertyEntity
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#Timeline");

	/**
	 * An identifier of the entry in the source timeline (e.g. Twitter id).
	 * <p>
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#sourceId>}
	 */
	private String sourceId;

	/** Inverse collection for Entry.sourceTimeline. */
	private Set<Entry> entries;


	public Timeline(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return Timeline.CLASS_IRI;
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public Set<Entry> getEntries() {
		return (entries == null) ? new HashSet<>() : entries;
	}

	public void addEntry(Entry entry) {
		if (entries == null) entries = new HashSet<>();
		entries.add(entry);
		entry.setSourceTimeline(this);
	}

	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
		addValue(model, TA.sourceId, sourceId);
		addCollectionData(model, entries);
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
		if (getSourceId() != null) {
			properties.add("sourceId");
			properties.add(getSourceId());
		}
		return properties.toArray();
	}

	@Override
	protected Set<RdfPropertyEntity> getEntities() {
		return Collections.unmodifiableSet(getEntries());
	}
}
