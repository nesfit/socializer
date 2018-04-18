package cz.vutbr.fit.xtutko00.source.twitter;

import cz.vutbr.fit.xtutko00.model.rdf.*;
import cz.vutbr.fit.xtutko00.source.SourceClient;
import cz.vutbr.fit.xtutko00.source.twitter.model.TwitterEntityFactory;
import cz.vutbr.fit.xtutko00.utils.Logger;
import org.apache.commons.collections.CollectionUtils;
import twitter4j.*;

import java.util.ArrayList;
import java.util.List;

public class TwitterSourceClient implements SourceClient {

    private static final int MAX_PAGE_SIZE = 200;
    private static final int RATE_LIMIT_SLEEP_INTERVAL = 15000;

    private final Logger logger = new Logger(TwitterSourceClient.class);

    private Twitter twitter;

    public TwitterSourceClient(Twitter twitter) {
        this.twitter = twitter;
    }

    @Override
    public Timeline getTimeline(String userName) {
        if (twitter == null) {
            logger.error("No twitter session given.");
            return null;
        }

        try {
            logger.info("START: Downloading tweets from " + userName);
            List<Status> userStatuses = loadUserStatuses(userName, twitter);
            logger.info("END: Downloading tweets from " + userName + ". Tweets count: " + userStatuses.size());

            if (CollectionUtils.isEmpty(userStatuses)) {
                logger.error("No Tweets downloaded from user " + userName);
                return null;
            }

            return createTimeline(userName, userStatuses);
        } catch (TwitterException e) {
            logger.error("END: Downloading tweets from " + userName + ". Cannot download user Tweets: " + e.getMessage());
            return null;
        }
    }

    /**
     * Load all statuses of given user from Twitter.
     */
    private List<Status> loadUserStatuses(String user, Twitter twitter) throws TwitterException {

        Paging p = new Paging(1, MAX_PAGE_SIZE);

        List<Status> newStatuses = null;
        while(newStatuses == null) {
            try {
                newStatuses = twitter.getUserTimeline(user, p);
            } catch (TwitterException e) {
                if (e.exceededRateLimitation()) {
                    handleRateLimitExceeded();
                } else {
                    throw e;
                }
            }

        }

        List<Status> statuses = new ArrayList<>();
        statuses.addAll(newStatuses);

        while (newStatuses.size() == MAX_PAGE_SIZE) {
            try {
                p.setPage(p.getPage() + 1);
                newStatuses = twitter.getUserTimeline(user, p);
                statuses.addAll(newStatuses);
            } catch (TwitterException e) {
                if (e.exceededRateLimitation()) {
                    p.setPage(p.getPage() - 1);
                    handleRateLimitExceeded();
                } else {
                    throw e;
                }
            }
        }

        return statuses;
    }

    /**
     * @author burgetr
     */
    private Timeline createTimeline(String username, List<Status> statuses) {

        final TwitterEntityFactory ef = TwitterEntityFactory.getInstance();
        Timeline timeline = ef.createTimeline(username);
        timeline.setSourceId(username);

        for (Status status : statuses)
        {
            Entry entry = ef.createEntry(status.getId());
            entry.setSourceId(String.valueOf(status.getId()));
            entry.setTimestamp(status.getCreatedAt());
            timeline.addEntry(entry);

            //text
            if (status.getText() != null)
            {
                TextContent tc = ef.createTextContent(status.getId());
                tc.setText(status.getText());
                entry.getContains().add(tc);
            }
            //images
            MediaEntity[] media = status.getMediaEntities();
            if (media != null)
            {
                for (MediaEntity entity : media)
                {
                    if (entity.getType().equals("photo"))
                    {
                        Image img = ef.createImage(entity.getId());
                        img.setSourceUrl(entity.getMediaURL());
                        entry.getContains().add(img);
                    }
                }
            }
            //URLs
            URLEntity[] urls = status.getURLEntities();
            if (urls != null)
            {
                for (URLEntity entity : urls)
                {
                    URLContent url = ef.createURLContent(status.getId(), entity.getStart());
                    url.setSourceUrl(entity.getExpandedURL());
                    url.setText(entity.getDisplayURL());
                    entry.getContains().add(url);
                }
            }
            //GEO entries
            GeoLocation loc = status.getGeoLocation();
            if (loc != null)
            {
                GeoContent geo = ef.createGeoContent(status.getId());
                geo.setLatitude(loc.getLatitude());
                geo.setLongitude(loc.getLongitude());
                entry.getContains().add(geo);
            }

        }

        return timeline;
    }

    private void handleRateLimitExceeded() {
        logger.warning("Twitter rate limit exceeded, sleeping for " + RATE_LIMIT_SLEEP_INTERVAL);
        try {
            Thread.sleep(RATE_LIMIT_SLEEP_INTERVAL);
        } catch (InterruptedException e) {
            logger.warning("Sleep resumed.");
        }
    }
}
