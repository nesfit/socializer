package cz.vutbr.fit.xtutko00;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

public class TestHalyard {

    public static void main(String[] args) {
        String rdf4jServer = "http://localhost:9876/rdf4j-server/";
        String repositoryID = "fb_timeline_kubo114";
        Repository repo = new HTTPRepository(rdf4jServer, repositoryID);
        repo.initialize();

        try (RepositoryConnection con = repo.getConnection()) {
            String queryString =
                    "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                    "PREFIX ex: <http://example.org/> " +
                    "select ?s ?p ?o " +
                    "where {?s ?p ?o} ";

            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
            try (TupleQueryResult result = tupleQuery.evaluate()) {
                while (result.hasNext()) {  // iterate over the result
                    BindingSet bindingSet = result.next();
                    Value valueOfX = bindingSet.getValue("x");
                    Value valueOfY = bindingSet.getValue("y");
                    // do something interesting with the values here...
                }
            }
        }
    }
}
