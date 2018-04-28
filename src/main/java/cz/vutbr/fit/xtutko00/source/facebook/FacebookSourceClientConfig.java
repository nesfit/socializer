package cz.vutbr.fit.xtutko00.source.facebook;

import cz.vutbr.fit.xtutko00.source.SourceClientConfig;

/**
 * Facebook source client config.
 *
 * @author xtutko00
 */
public class FacebookSourceClientConfig extends SourceClientConfig {

    private String untilDate;

    public FacebookSourceClientConfig(String key, String secret, String untilDate) {
        super(key, secret);
        this.untilDate = untilDate;
    }

    public String getUntilDate() {
        return untilDate;
    }
}
