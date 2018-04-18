package cz.vutbr.fit.xtutko00.source;

import java.io.Serializable;

public class Source implements Serializable {

    private ESourceType type;
    private String name;

    public Source(ESourceType type, String name) {
        this.type = type;
        this.name = name;
    }

    public ESourceType getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
