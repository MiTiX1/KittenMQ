package org.kittenmq.errors;

import java.util.logging.Logger;

public class ErrorHandler {
    private static final Logger logger = Logger.getLogger(ErrorHandler.class.getName());

    public static void logError(String message, Exception e) {
        logger.severe(message + " - Error: " + e.getMessage());
    }

    public static void logWarning(String message) {
        logger.warning(message);
    }
}
