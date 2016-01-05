package com.blu.database;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import com.blu.database.db.DatabaseProducer;
import com.blu.database.db.DbContract;
import com.blu.database.db.PcbDatabaseConsumer;
import com.blu.database.db.SysDatabaseConsumer;

public class Main {
    private static final String sClassName = Main.class.getName();
    private static final Logger LOGGER = Logger.getLogger(sClassName);
    private static int NUM_CONSUMER_THREADS = 1;
    
	public static void main(String[] args) {
		BlockingQueue<File> pcbFilenamesQueue = new LinkedBlockingQueue<>();
		BlockingQueue<File> sysFilenamesQueue = new LinkedBlockingQueue<>();
		
		try {
            LOGGER.addHandler(new FileHandler("./" + sClassName + ".log",
                                  true /*append*/));
        } catch (SecurityException | IOException e) {
            LOGGER.severe(e.getStackTrace().toString());
        }

		// start PCB producer to produce messages in queue
		new Thread(
	        new DatabaseProducer(pcbFilenamesQueue, 
                "C:/projects/BluDatabase/testData/PCB - Line Software", 
                NUM_CONSUMER_THREADS)).start();
		  
		// start PCB consumers to consume messages from queue
		for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
    		new Thread(
		        new PcbDatabaseConsumer(
        			DbContract.HOST, DbContract.DB_NAME, DbContract.USERNAME,
        			DbContract.PASSWORD, DbContract.PCB_TESTS_TABLE, 
        			pcbFilenamesQueue)).start();
		}
		
        // start SYS producer to produce messages in queue
        new Thread(
            new DatabaseProducer(sysFilenamesQueue, 
                "C:/projects/BluDatabase/testData/System Test", 
                NUM_CONSUMER_THREADS)).start();
          
        // start SYS consumers to consume messages from queue
        for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
            new Thread(
                new SysDatabaseConsumer(
                    DbContract.HOST, DbContract.DB_NAME, DbContract.USERNAME,
                    DbContract.PASSWORD, DbContract.SYS_TESTS_TABLE, 
                    sysFilenamesQueue)).start();
        }
        
        LOGGER.info("Producer and Consumers have  been started");
	}
}
