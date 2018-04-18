package cz.vutbr.fit.xtutko00.utils;

import java.sql.Timestamp;

/**
 * Id generator.
 *
 * @author xtutko00
 */
public class IdMaker {

    private String idPrefix;
    private Integer counter;
    private String timestamp;

    public IdMaker(String idPrefix) {
        this.idPrefix = idPrefix;
        this.counter = 0;
        this.timestamp = String.valueOf(new Timestamp(System.currentTimeMillis()).getTime());
    }

    /**
     * Returns unique ID based on given prefix, current timestamp and counter value.
     */
    public String getId() {
        return idPrefix + "_" + timestamp + "_" + counter++;
    }
}
