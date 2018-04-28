package cz.vutbr.fit.xtutko00.source;

import java.io.Serializable;

/**
 * Basic source client configuration.
 *
 * @author xtutko00
 */
public abstract class SourceClientConfig implements Serializable {

    private String key;
    private String secret;

    public SourceClientConfig(String key, String secret) {
        this.key = key;
        this.secret = secret;
    }

    public String getKey() {
        return key;
    }

    public String getSecret() {
        return secret;
    }
}
