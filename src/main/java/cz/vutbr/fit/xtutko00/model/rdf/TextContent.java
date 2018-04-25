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
		if (StringUtils.isNotBlank(getText())) {
			properties.add("text");
			properties.add(getText());
		}
		return properties.toArray();
	}
}
