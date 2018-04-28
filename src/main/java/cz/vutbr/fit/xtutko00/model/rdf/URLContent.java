package cz.vutbr.fit.xtutko00.model.rdf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;

import cz.vutbr.fit.xtutko00.model.core.EntityFactory;
import cz.vutbr.fit.xtutko00.model.rdf.vocabulary.TA;
import cz.vutbr.fit.xtutko00.utils.IdMaker;

/**
 * UrlContent.
 *
 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author xtutko00
 */
public class URLContent extends Content
{
	public static final IRI CLASS_IRI = vf.createIRI("http://nesfit.github.io/ontology/ta.owl#URLContent");

	/**
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#text>}
	 */
	private String text;

	/**
	 * Source URL of the media..
	 * <p>
	 * IRI: {@code <http://nesfit.github.io/ontology/ta.owl#sourceUrl>}
	 */
	private String sourceUrl;


	public URLContent(IRI iri) {
		super(iri);
	}

	@Override
	public IRI getClassIRI() {
		return URLContent.CLASS_IRI;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	/**
	 * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
	 *
	 * @author burgetr
	 */
	@Override
	public void addToModel(Model model) {
		super.addToModel(model);
		addValue(model, TA.text, text);
		addValue(model, TA.sourceUrl, sourceUrl);
	}

	/**
	 * Returns UrlContent properties.
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
		if (getText() != null) {
			properties.add("text");
			properties.add(getText());
		}
		if (StringUtils.isNotBlank(getSourceUrl())) {
			properties.add("sourceUrl");
			properties.add(getSourceUrl());
		}
		return properties.toArray();
	}
}
