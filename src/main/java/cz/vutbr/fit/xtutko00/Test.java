package cz.vutbr.fit.xtutko00;

import cz.vutbr.fit.xtutko00.model.rdf.Timeline;
import cz.vutbr.fit.xtutko00.source.ESourceType;
import cz.vutbr.fit.xtutko00.source.SourceClient;
import cz.vutbr.fit.xtutko00.source.SourceClientFactory;
import cz.vutbr.fit.xtutko00.source.facebook.FacebookSourceClient;
import cz.vutbr.fit.xtutko00.source.facebook.FacebookSourceClientConfig;
import cz.vutbr.fit.xtutko00.utils.XmlResourceParser;

import java.util.Map;

public class Test {

    private static final String RESOURCE_FACEBOOK_APP_ID = "facebook.app.id";
    private static final String RESOURCE_FACEBOOK_APP_SECRET = "facebook.app.secret";
    private static final String RESOURCE_FACEBOOK_UNTIL_DATE = "facebook.date.until";

    public static void main(String[] args) {
        Map<String, String> properties = new XmlResourceParser("/configuration.xml").parse();

        FacebookSourceClientConfig facebookConfig = getFacebookConfig(properties);

        SourceClientFactory sourceClientFactory = new SourceClientFactory(facebookConfig, null);

        SourceClient facebookSourceClient = sourceClientFactory.createSourceClient(ESourceType.FACEBOOK);

        Timeline timeline = facebookSourceClient.getTimeline("Kubo114");

        System.out.println("Konec");
    }

    private static FacebookSourceClientConfig getFacebookConfig(Map<String, String> properties) {
        String appId = properties.get(RESOURCE_FACEBOOK_APP_ID);
        if (appId == null) {
            return null;
        }

        String appSecret = properties.get(RESOURCE_FACEBOOK_APP_SECRET);
        if (appSecret == null) {
            return null;
        }

        String untilDate = properties.get(RESOURCE_FACEBOOK_UNTIL_DATE);

        return new FacebookSourceClientConfig(appId, appSecret, untilDate);
    }
}
