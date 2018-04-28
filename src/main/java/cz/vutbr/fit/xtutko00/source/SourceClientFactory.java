package cz.vutbr.fit.xtutko00.source;

import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Version;
import com.restfb.exception.FacebookOAuthException;
import cz.vutbr.fit.xtutko00.source.facebook.FacebookSourceClient;
import cz.vutbr.fit.xtutko00.source.facebook.FacebookSourceClientConfig;
import cz.vutbr.fit.xtutko00.source.twitter.TwitterSourceClient;
import cz.vutbr.fit.xtutko00.source.twitter.TwitterSourceClientConfig;
import cz.vutbr.fit.xtutko00.utils.Logger;
import org.apache.commons.lang.StringUtils;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.Serializable;

/**
 * Creates source clients.
 *
 * @author xtutko00
 */
public class SourceClientFactory implements Serializable {

    private final Logger logger = new cz.vutbr.fit.xtutko00.utils.Logger(SourceClientFactory.class);

    private FacebookSourceClientConfig facebookConfig;
    private TwitterSourceClientConfig twitterConfig;

    public SourceClientFactory(FacebookSourceClientConfig facebookConfig, TwitterSourceClientConfig twitterConfig) {
        this.facebookConfig = facebookConfig;
        this.twitterConfig = twitterConfig;
    }

    public SourceClient createSourceClient(ESourceType type) {
        switch (type) {
            case FACEBOOK:
                if (areCredentialsSet(facebookConfig)) {
                    logger.error("Cannot create FacebookSource because no FB credentials were given.");
                    return null;
                }

                FacebookClient facebookClient = createFacebookSession(facebookConfig);
                return new FacebookSourceClient(facebookClient, facebookConfig.getUntilDate());
            case TWITTER:
                if (areCredentialsSet(twitterConfig)) {
                    logger.error("Cannot create TwitterSource because no Twitter credentials were given.");
                    return null;
                }

                Twitter twitter = createTwitterSession(twitterConfig);
                return new TwitterSourceClient(twitter);
            default:
                return null;
        }
    }

    /**
     * Creates connection to Facebook.
     */
    private FacebookClient createFacebookSession(FacebookSourceClientConfig config) {
        try {
            FacebookClient facebookClient = new DefaultFacebookClient(Version.VERSION_2_12);
            FacebookClient.AccessToken accessToken = facebookClient.obtainAppAccessToken(config.getKey(), config.getSecret());
            return new DefaultFacebookClient(accessToken.getAccessToken(), Version.VERSION_2_12);
        } catch (FacebookOAuthException e) {
            return null;
        }
    }

    /**
     * Creates connection to Twitter.
     */
    private Twitter createTwitterSession(TwitterSourceClientConfig config) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setApplicationOnlyAuthEnabled(true)
                .setOAuthConsumerKey(config.getKey())
                .setOAuthConsumerSecret(config.getSecret());

        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        try {
            twitter.getOAuth2Token();
        } catch (TwitterException e) {
            return null;
        }

        return twitter;
    }

    private boolean areCredentialsSet(SourceClientConfig config) {
        return config == null || StringUtils.isBlank(config.getKey()) || StringUtils.isBlank(config.getSecret());
    }
}
