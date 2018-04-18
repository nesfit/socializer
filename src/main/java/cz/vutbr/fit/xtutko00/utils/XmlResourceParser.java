package cz.vutbr.fit.xtutko00.utils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Parses keys and values from resources formatted as XML.
 * Only valid XML format is:
 * <configuration>
 *  <property>
 *      <name>...</name>
 *      <value>...</value>
 *  </property>
 * </configuration>
 *
 * @author xtutko00
 */
public class XmlResourceParser implements Serializable {

    private final Logger logger = new Logger(XmlResourceParser.class);

    private static final String PROPERTY_NAME = "property";
    private static final String KEY_NAME = "name";
    private static final String VALUE_NAME = "value";

    private String fileName;

    public XmlResourceParser(String fileName) {
        this.fileName = fileName;
    }

    public Map<String, String> parse() {
        try {
            InputStream stream = getClass().getResourceAsStream(fileName);
            if (stream == null) {
                logger.error("Cannot find resource file " + this.fileName);
                return new HashMap<>();
            }

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(stream);

            doc.getDocumentElement().normalize();

            Map<String, String> keyValues = new HashMap<>();

            NodeList nList = doc.getElementsByTagName(PROPERTY_NAME);
            for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                    Element eElement = (Element) nNode;

                    String key = eElement.getElementsByTagName(KEY_NAME).item(0).getTextContent();
                    String value = eElement.getElementsByTagName(VALUE_NAME).item(0).getTextContent();
                    keyValues.put(key, value);
                }
            }

            return keyValues;
        } catch (Exception e) {
            logger.error("Cannot parse xml file: " + e.getMessage());
            e.printStackTrace();
            return new HashMap<>();
        }
    }

}
