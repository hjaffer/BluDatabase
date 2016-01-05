package com.blu.database.db;

import java.io.File;
import java.util.concurrent.BlockingQueue;

public class DatabaseProducer implements Runnable {
    public static String END_STRING = "<EXIT>";
    private static String FILE_EXT_TO_PRODUCE = ".csv";
    private BlockingQueue<File> mFilenameQueue;
    private File mDirectory;
    private int mNumConsumerThreads;

    public DatabaseProducer(BlockingQueue<File> filenameQueue,
                            String directory, int numConsumerThreads) {
        this.mFilenameQueue = filenameQueue;
        this.mNumConsumerThreads = numConsumerThreads;
        this.mDirectory = new File(directory);
    }
    
    public void queueFilesInDirectory(File directory) {
        // get all the files from the directory
        File[] filesInDir = directory.listFiles();
        for (File curFile : filesInDir) {
            if (curFile.isFile()) { // file
                if (curFile.getAbsolutePath().toLowerCase().endsWith(
                        FILE_EXT_TO_PRODUCE)) {
                    mFilenameQueue.add(curFile);
                }
            } else if (curFile.isDirectory()) { // directory
                queueFilesInDirectory(curFile);
            }
        }
    }
    
    @Override
    public void run() {
        queueFilesInDirectory(this.mDirectory);
        for (int i = 0; i < mNumConsumerThreads; i++) {
            mFilenameQueue.add(new File("<EXIT>"));
        }
    }
}
