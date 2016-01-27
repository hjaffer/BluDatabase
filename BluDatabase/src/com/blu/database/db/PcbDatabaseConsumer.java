package com.blu.database.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class PcbDatabaseConsumer extends DatabaseConsumer {
    private static final String sClassName = PcbDatabaseConsumer.class.getName();
    private static final Logger LOGGER = Logger.getLogger(sClassName);

    public PcbDatabaseConsumer(String host, String dbName, String username, 
            String password, String table, BlockingQueue<File> filenamesQueue) {
        super(host, dbName, username,password, table, filenamesQueue);
        try {
            LOGGER.addHandler(new FileHandler("./" + sClassName + ".log",
                                  true /*append*/));
        } catch (SecurityException | IOException e) {
            LOGGER.severe(e.getStackTrace().toString());
        }
    }

    @Override
    public void run() {
        try {
            // connect to the database
            this.connect();
            LOGGER.info("Successfully connected to the database");
        } catch (ClassNotFoundException | SQLException e) {
            LOGGER.severe(e.toString());
        }
        
        // consume messages until exit file is received
        String query = null;
        while (true) {
            try {
                File curFile = mFilenameQueue.take();
                String curFilename = curFile.getName();
                String curFileAbsPath = curFile.getAbsolutePath();
                
                // check if the file has already been processed
                if (fileAlreadyProcessed(curFilename)) {
                    LOGGER.warning("File already processed: " + curFilename);
                    continue;
                }
                
                // determine whether to stop consuming files
                if (curFileAbsPath.endsWith(DatabaseProducer.END_STRING)) {
                    LOGGER.info("Finished processing data files");
                    break;
                }
                
                // start processing the file
                LOGGER.info("Processing file: " + curFileAbsPath);
                try (BufferedReader br = 
                        new BufferedReader(
                            new InputStreamReader(
                                new FileInputStream(curFileAbsPath)))) {
                    String logfileTitleLine = br.readLine();
                    if (logfileTitleLine == null || logfileTitleLine.isEmpty()) {
                        LOGGER.severe("Error processing line: " + logfileTitleLine);
                        continue;
                    }
                    String[] logfileTitleArr = logfileTitleLine.split(",");
                    if (logfileTitleArr.length <= 0) {
                        LOGGER.severe("Error processing line: " + logfileTitleLine);
                        continue;
                    }
                    String logFileTitle = logfileTitleArr[0].replace("Title:", "").trim();
                    
                    // read the remaining lines of the file header
                    for (int i = 0; i < 3; i++) {
                        String curHeaderLine = br.readLine();
                        if (curHeaderLine == null || curHeaderLine.isEmpty()) {
                            LOGGER.severe("Error processing headers");
                            continue;
                        }
                    }
                    
                    // get the column keys
                    String keys = br.readLine();
                    if (keys == null || keys.isEmpty()) {
                        LOGGER.severe("Error processing keys: " + keys);
                        continue;
                    }

                    // add the "Current Tester ID" if not present
                    boolean curTesterIdPresent = true;
                    if (!keys.contains("Current Tester ID")) {
                        keys = keys.replace("Prev Tester ID", "Prev Tester ID,Current Tester ID");
                        curTesterIdPresent = false;
                    }
                    
                    String[] arrKeys = keys.split(",");
                    StringBuilder keyBldr = new StringBuilder();
                    for (String curKey : arrKeys) {
                        keyBldr.append("\"" + curKey.trim() + "\",");
                    }
                    keyBldr.append("\"Log Filename\",");
                    keyBldr.append("\"Log Title\"");
                    
                    // read the file line by line                   
                    String curDataLine;
                    StringBuilder valBldr = new StringBuilder();
                    while ((curDataLine = br.readLine()) != null) {
                        // finished processing the file (below is summary information)
                        if (curDataLine.contains(",,,,,")) {
                            break;
                        }
                        
                        if (!curTesterIdPresent) {
                            int insNdx = nthIndexOf(curDataLine, ',', 6);
                            curDataLine = curDataLine.substring(0, insNdx) + 
                                          ",-1" + 
                                          curDataLine.substring(
                                              insNdx, curDataLine.length());
                        }
                        
                        String[] arrVals = curDataLine.split(",");
                        
                        // make sure the keys and values match
                        if (arrKeys.length != arrVals.length) {
                            LOGGER.severe(arrKeys.length + "!=" +
                                arrVals.length + ", Error processing line: " + 
                                curDataLine);
                            LOGGER.severe(Arrays.toString(arrKeys));
                            
                            continue;
                        }
                        
                        valBldr.setLength(0);                        
                        for (String curVal : arrVals) {
                            valBldr.append("'" + curVal.trim() + "',");
                        }
                        valBldr.append("'" + curFilename + "',");
                        valBldr.append("'" + logFileTitle + "'");
                        
                        // insert the data into the database
                        query = 
                            String.format("INSERT INTO %s (%s) VALUES (%s)", 
                                          mTable, keyBldr.toString(), 
                                          valBldr.toString());
                        // System.out.println(query);
                        try {
                            this.mConn.createStatement().executeUpdate(query);
                        } catch (SQLException e) {
                            LOGGER.severe(e.toString());
                            LOGGER.severe("Query is: " + query);
                        }
                    }
                } catch (FileNotFoundException e) {
                    LOGGER.severe(e.toString());
                } catch (IOException e) {
                    LOGGER.severe(e.toString());
                }
            } catch(InterruptedException e) {
                LOGGER.severe(e.toString());
            } catch (SQLException e) {
                LOGGER.severe(e.toString());
                LOGGER.severe("Query is: " + query);
            }
        }
        LOGGER.info("PCB Consumer finished.");
    }
    
    public static int nthIndexOf(String text, char needle, int n)
    {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == needle) {
                n--;
                if (n == 0) {
                    return i;
                }
            }
        }
        
        return -1;
    }
}

/*
-- Table: public.pcb_test_logs

-- DROP TABLE public.pcb_test_logs;

CREATE TABLE public.pcb_test_logs
(
  id SERIAL PRIMARY KEY,
  "Error Code" smallint,
  "Error Description" character varying(256),
  "TimeStamp" timestamp without time zone,
  "Serial No" character varying(32),
  "Re-test" boolean,
  "Prev Tester ID" smallint,
  "Current Tester ID" smallint,
  "Software Ver." character varying(16),
  "Fixture Firmware Ver." character varying(16),
  "UUT Firmware Ver." character varying(16),
  "Operator ID" character varying(100),
  "Board Rev" smallint,
  "Limits File" character varying(256),
  "Limits File Version" character varying(16),
  "Last Test Step" character varying(256),
  "Avg Vref counts" real,
  "Internal Bat Voltage" real,
  "Min Cal Factor" real,
  "Cal Factor" real,
  "Max Cal Factor" real,
  "Min Cig Detect Voltage" real,
  "Cig Detect Voltage" real,
  "Max Cig Detect Voltage" real,
  "Min Cig Battery Voltage" real,
  "Cig Battery Voltage" real,
  "Max Cig Battery Voltage" real,
  "Min Cig Charge Current" real,
  "Cig Charge Current" real,
  "Max Cig Charge Current" real,
  "Blue Led Baseline" smallint,
  "Blue Led Off Reading" smallint,
  "Blue Led On Reading" smallint,
  "Blue Led On Threshold" smallint,
  "Red Led Baseline" smallint,
  "Red Led Off Reading" smallint,
  "Red Led On Reading" smallint,
  "Red Led On Threshold" smallint,
  "Log Filename" character varying(256),
  "Log Title" character varying(256)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.pcb_test_logs
  OWNER TO postgres;
*/