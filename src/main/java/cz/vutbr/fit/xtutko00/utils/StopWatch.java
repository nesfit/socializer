package cz.vutbr.fit.xtutko00.utils;

/**
 * Created by jakubmac on 25/04/2018.
 */
public class StopWatch {

    private Long startTime = 0L;
    private Long stopTime = 0L;

    public void start() {
        startTime = System.currentTimeMillis();
        stopTime = 0L;
    }

    public void stop() {
        if (stopTime == 0L) {
            stopTime = System.currentTimeMillis();
        }
    }

    public Long getTimeSec() {
        long elapsedTimeMillis = stopTime - startTime;
        float elapsedTimeSec = elapsedTimeMillis/1000F;
        return (long) elapsedTimeSec;
    }

    public Long getTimeMillis() {
        return stopTime - startTime;
    }
}
