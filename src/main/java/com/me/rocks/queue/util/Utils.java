package com.me.rocks.queue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Utils {
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    /**
     * Recursively delete a folder
     * @param directoryToBeDeleted
     * @return
     */
    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void mkdirIfNotExists(String path) {
        File file = new File(path);
        if(!file.exists()) {
            if(!file.mkdirs()) {
                log.error("Failed to create rocks store directory {}", path);
            }
        }
    }

    public static boolean nullOrEmpty(String directory) {
        return directory == null || directory.trim().isEmpty();
    }


}
