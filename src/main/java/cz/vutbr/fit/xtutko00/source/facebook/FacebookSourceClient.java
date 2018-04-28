package cz.vutbr.fit.xtutko00.source.facebook;

import com.restfb.ConnectionIterator;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.exception.FacebookNetworkException;
import com.restfb.types.Location;
import com.restfb.types.Post;
import com.restfb.types.StoryAttachment;
import cz.vutbr.fit.xtutko00.model.rdf.*;
import cz.vutbr.fit.xtutko00.source.SourceClient;
import cz.vutbr.fit.xtutko00.source.facebook.model.FBEntityFactory;
import cz.vutbr.fit.xtutko00.utils.Logger;
import org.apache.commons.collections.CollectionUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Client for Facebook social network.
 *
 * Some of the methods reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
 *
 * @author xtutko00
 */
public class FacebookSourceClient implements SourceClient {

    private final Logger logger = new cz.vutbr.fit.xtutko00.utils.Logger(FacebookSourceClient.class);

    private static final String FEED_FIELDS = "created_time,message,link,place,attachments";
    private static final int MAX_PAGE_SIZE = 100;

    private static final int RATE_LIMIT_ERROR_CODE1 = 4;
    private static final int RATE_LIMIT_ERROR_CODE2 = 17;
    private static final int RATE_LIMIT_SLEEP_INTERVAL = 15000;

    private FacebookClient facebook;
    private String untilDate;

    public FacebookSourceClient(FacebookClient facebook, String untilDate) {
        this.facebook = facebook;

        if (untilDate != null) {
            try {
                DateFormat format = new SimpleDateFormat("yyyy-mm-dd");
                format.parse(untilDate);
                this.untilDate = untilDate;
            } catch (ParseException e) {
                logger.error("Cannot convert date string \"" + untilDate + "\" to date with format yyy-mm-dd");
            }
        }
    }

    /**
     * Downloads timeline of given Facebook page.
     *
     * @param pageName text id of FB page
     */
    @Override
    public Timeline getTimeline(String pageName) {
        if (facebook == null) {
            logger.error("No facebook session given.");
            return null;
        }

        try {
            logger.info("START: Downloading posts of page " + pageName);
            List<Post> feed = loadFeed(pageName, facebook);
            logger.info("END: Downloading posts of page " + pageName + ". Downloaded " + feed.size() + " posts.");

            if (CollectionUtils.isEmpty(feed)) {
                logger.error("No feeds downloaded from page " + pageName);
                return null;
            }

            return createTimeline(pageName, feed);
        } catch (Exception e) {
            logger.error("END: Downloading posts of page " + pageName + ". Cannot download timeline: " + e.getMessage());
            return null;
        }
    }

    /**
     * Downloads page posts.
     */
    private List<Post> loadFeed(String pageName, FacebookClient facebook) {

        String dest = pageName + "/feed";

        List<Parameter> plist = new ArrayList<>();
        plist.add(Parameter.with("fields", FEED_FIELDS));
        plist.add(Parameter.with("limit", MAX_PAGE_SIZE));
        if (untilDate != null) {
            plist.add(Parameter.with("until", untilDate));
        }
        Parameter[] params = new Parameter[plist.size()];
        params = plist.toArray(params);

        // Accessing first page
        ConnectionIterator<Post> feedItr = null;
        while(feedItr == null) {
            try {
                feedItr = facebook.fetchConnection(dest, Post.class, params).iterator();
            } catch (FacebookNetworkException e) {
                if (e.getHttpStatusCode() == RATE_LIMIT_ERROR_CODE1 || e.getHttpStatusCode() == RATE_LIMIT_ERROR_CODE2) {
                    handleRateLimitExceeded();
                } else {
                    throw e;
                }
            }
        }

        // Accessing the rest of the posts
        List<Post> feeds = new ArrayList<>();
        while(feedItr.hasNext()) {
            try {
                List<Post> actualFeeds = feedItr.next();
                feeds.addAll(actualFeeds);
            } catch (FacebookNetworkException e) {
                if (e.getHttpStatusCode() == RATE_LIMIT_ERROR_CODE1 || e.getHttpStatusCode() == RATE_LIMIT_ERROR_CODE2) {
                    handleRateLimitExceeded();
                } else {
                    throw e;
                }
            }
        }

        return feeds;
    }

    /**
     * Converts posts into Timeline.
     *
     * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
     *
     * @author burgetr
     */
    private Timeline createTimeline(String pageName, List<Post> feed) {

        final FBEntityFactory ef = FBEntityFactory.getInstance();
        Timeline timeline = ef.createTimeline(pageName);
        timeline.setSourceId(pageName);

        for (Post post : feed) {
            Entry entry = createEntry(ef, post);
            if (entry != null) {
                timeline.addEntry(entry);
            }
        }

        return timeline;
    }

    /**
     * Converts post into Entry.
     *
     * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
     *
     * @author burgetr
     */
    private Entry createEntry(FBEntityFactory ef, Post post)
    {
        Entry entry = ef.createEntry(post.getId());
        entry.setSourceId(post.getId());
        entry.setTimestamp(post.getCreatedTime());

        //text
        if (post.getMessage() != null)
        {
            TextContent tc = ef.createTextContent(post.getId());
            tc.setText(post.getMessage());
            entry.getContains().add(tc);
        }
        //URLs
        if (post.getLink() != null)
        {
            URLContent uc = ef.createURLContent(post.getId(), 0); //only one link per post(?)
            uc.setSourceUrl(post.getLink());
            if (post.getDescription() != null)
                uc.setText(post.getDescription());
            else if (post.getName() != null)
                uc.setText(post.getName());
            else if (post.getCaption() != null)
                uc.setText(post.getCaption());
            else
                uc.setText("???");
            entry.getContains().add(uc);
        }
        //Images
        Post.Attachments atts = post.getAttachments();
        if (atts != null)
        {
            int icnt = 0;
            for (StoryAttachment att : atts.getData())
            {
                if (att.getMedia() != null)
                {
                    StoryAttachment.Image img = att.getMedia().getImage();
                    if (img != null)
                        entry.getContains().add(createImageFromAttachment(ef, img, post.getId(), icnt++));
                }
                //subattachments
                StoryAttachment.Attachments satts = att.getSubAttachments();
                if (satts != null)
                {
                    for (StoryAttachment satt : satts.getData())
                    {
                        if (satt.getMedia() != null)
                        {
                            StoryAttachment.Image img = satt.getMedia().getImage();
                            if (img != null)
                                entry.getContains().add(createImageFromAttachment(ef, img, post.getId(), icnt++));
                        }
                    }
                }
            }
        }
        //GEO entries
        if (post.getPlace() != null && post.getPlace().getLocation() != null)
        {
            Location loc = post.getPlace().getLocation();
            if (loc.getLatitude() != null && loc.getLongitude() != null)
            {
                GeoContent gc = ef.createGeoContent(post.getId());
                gc.setLatitude(loc.getLatitude());
                gc.setLongitude(loc.getLongitude());
                entry.getContains().add(gc);
            }
        }

        return entry;
    }

    /**
     * Converts post's image into Image.
     *
     * Reused from <a href="https://github.com/nesfit/timeline-analyzer">timeline-analyzer</a>
     *
     * @author burgetr
     */
    private Image createImageFromAttachment(FBEntityFactory ef, StoryAttachment.Image img, String postId, int imgCnt)
    {
        Image ret = ef.createImage(postId + "-img-" + imgCnt);
        ret.setSourceUrl(img.getSrc());
        return ret;
    }

    /**
     * Handling exceeded rate limit.
     */
    private void handleRateLimitExceeded() {
        logger.warning("Facebook rate limit exceeded. Sleeping for " + RATE_LIMIT_SLEEP_INTERVAL + " milliseconds.");
        try {
            Thread.sleep(RATE_LIMIT_SLEEP_INTERVAL);
        } catch (InterruptedException e) {
            logger.warning("Sleep resumed.");
        }
    }
}
