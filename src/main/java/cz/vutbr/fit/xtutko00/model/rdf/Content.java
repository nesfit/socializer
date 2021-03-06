package cz.vutbr.fit.xtutko00.model.rdf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;
import cz.vutbr.fit.xtutko00.model.property.RdfPropertyEntity;
import cz.vutbr.fit.xtutko00.utils.IdMaker;

/**
 * Content.
 *
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author xtutko00
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

	/**
	 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
	 *
	 * @author burgetr
	 */
	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
	}

	/**
	 * Returns Content properties.
	 */
	@Override
	protected Object[] getProperties(IdMaker idMaker) {
		List<Object> properties = new ArrayList<>();

		properties.add(T.id);
		properties.add(idMaker.getId());
		properties.add(T.label);
		properties.add(buildLabel(getClassIRI()));

		if (StringUtils.isNotBlank(getLabel())) {
			properties.add("label");
			properties.add(getLabel());
		}

		return properties.toArray();
	}
}
