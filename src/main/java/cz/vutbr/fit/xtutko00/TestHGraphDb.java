package cz.vutbr.fit.xtutko00;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import cz.vutbr.fit.xtutko00.hbase.HBaseClientConfig;
import cz.vutbr.fit.xtutko00.utils.XmlResourceParser;
import io.hgraphdb.HBaseGraph;
import io.hgraphdb.HBaseGraphConfiguration;

/**
 * Test class for HGraphDB framework.
 *
 * @author xtutko00
 */
public class TestHGraphDb {

    private static final String HBASE_SITE_FILENAME = "/hbase-site.xml";

    /**
     * Main method.
     */
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("No args. Usage: $ [print/drop] [table_name]");
            return;
        }

        HBaseClientConfig hBaseClientConfig = new HBaseClientConfig()
                .setTableName(args[1])
                .setHBaseSiteFilename(HBASE_SITE_FILENAME);

        TestHGraphDb test = new TestHGraphDb(hBaseClientConfig);

        switch (args[0]) {
            case "print":
                test.printGraph();
                break;
            case "drop":
                test.drop();
                break;
        }
    }

    private HBaseClientConfig config;

    private TestHGraphDb(HBaseClientConfig config) {
        this.config = config;
    }

    /**
     * Prints all graph's vertices and edges.
     */
    private void printGraph() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration());

        System.out.println("START: Printing Vertices.");
        int vertexCounter = 0;
        Iterator<Vertex> iteratorVertex = graph.allVertices();
        while (iteratorVertex.hasNext()) {
            vertexCounter++;
            Vertex vertex = iteratorVertex.next();
            System.out.println();

            System.out.println("Vertex id: " + vertex.id());
            System.out.println("Vertex label: " + vertex.label());

            Iterator<VertexProperty<Object>> iteratorProperties = vertex.properties();
            while(iteratorProperties.hasNext()) {
                VertexProperty property = iteratorProperties.next();
                System.out.println("Property key: " + property.key());
                System.out.println("Property value: " + property.value());
            }
        }

        System.out.println();
        System.out.println("END: Printing Vertices. Size: " + vertexCounter);

        System.out.println("START: Printing Edges.");
        int edgesCounter = 0;
        Iterator<Edge> iteratorEdge = graph.allEdges();
        while (iteratorEdge.hasNext()) {
            edgesCounter++;
            Edge edge = iteratorEdge.next();
            System.out.println();

            System.out.println("Edge id: " + edge.id());
            System.out.println("Edge: " + edge.outVertex().label() + " " + edge.label() + " -> " + edge.inVertex().label());
            System.out.println("Edge: " + edge.outVertex().id() + " " + edge.label() + " -> " + edge.inVertex().id());
        }

        System.out.println();
        System.out.println("END: Printing Edges. Size: " + edgesCounter);

        System.out.println("Total vertexes: " + vertexCounter);
        System.out.println("Total edges: " + edgesCounter);

        graph.close();
    }

    /**
     * Removes all graph's vertices and edges. Do not delete HBase table.
     */
    private void drop() {
        HBaseGraph graph = (HBaseGraph) GraphFactory.open(getHBaseConfiguration());

        Iterator<Edge> iteratorEdge = graph.allEdges();
        while (iteratorEdge.hasNext()) {
            Edge edge = iteratorEdge.next();
            graph.removeEdge(edge);
        }

        Iterator<Vertex> iteratorVertex = graph.allVertices();
        while (iteratorVertex.hasNext()) {
            Vertex vertex = iteratorVertex.next();
            graph.removeVertex(vertex);
        }

        graph.close();
    }

    /**
     * Builds HBase configuration.
     */
    private Configuration getHBaseConfiguration() {
        HBaseGraphConfiguration cfg = new HBaseGraphConfiguration()
                .setInstanceType(HBaseGraphConfiguration.InstanceType.DISTRIBUTED)
                .setGraphNamespace(this.config.getTableName())
                .setCreateTables(true);

        XmlResourceParser hBaseConfigParser = new XmlResourceParser(this.config.getHBaseSiteFilename());
        Map<String, String> keyValues = hBaseConfigParser.parse();
        keyValues.forEach(cfg::set);

        return cfg;
    }
}
