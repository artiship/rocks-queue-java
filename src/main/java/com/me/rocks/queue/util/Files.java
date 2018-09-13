package com.me.rocks.queue.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Files {
    private static final Logger log = LoggerFactory.getLogger(Files.class);

    /**
     * Recursively delete a folder
     * @param directoryToBeDeleted
     * @return
     */
    public static boolean deleteDirectory(String directoryToBeDeleted) {
        File fd = new File(directoryToBeDeleted);

        if(!fd.exists()) {
            log.warn("The directory {} want to be deleted is not exists", directoryToBeDeleted);
            return false;
        }

        File[] allContents = fd.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file.getAbsolutePath());
            }
        }
        return fd.delete();
    }

    public static void mkdirIfNotExists(String path) {
        File file = new File(path);
        if(!file.exists()) {
            if(!file.mkdirs()) {
                log.error("Failed to create rocks store directory {}", path);
            }
        }
    }
}
