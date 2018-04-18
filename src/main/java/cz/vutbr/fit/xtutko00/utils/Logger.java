package cz.vutbr.fit.xtutko00.utils;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Basic lightweight logger used in Spark Jobs.
 *
 * @author xtutko00
 */
public class Logger implements Serializable {

    private static final int DEBUG = 0;
    private static final int INFO = 1;
    private static final int WARNING = 1;
    private static final int ERROR = 3;

    private final int severityLevel = 0;

    private String className;

    public Logger(Class logClass) {
        this.className = logClass.getSimpleName();
    }

    public void debug(String str) {
        if (severityLevel > DEBUG) {
            return;
        } else {
            log("DEBUG", str);
        }
    }

    public void info(String str) {
        if (severityLevel > INFO) {
            return;
        } else {
            log("INFO ", str);
        }
    }

    public void warning(String str) {
        if (severityLevel > WARNING) {
            return;
        } else {
            log("WARNING", str);
        }
    }

    public void error(String str) {
        if (severityLevel > ERROR) {
            return;
        } else {
            log("ERROR", str);
        }
    }

    private void log(String severity, String str) {
        String message = "[" + getCurrentDate() + "] " + severity + ": " + className + ": " + str;
        System.out.println(message);
    }

    private String getCurrentDate() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
