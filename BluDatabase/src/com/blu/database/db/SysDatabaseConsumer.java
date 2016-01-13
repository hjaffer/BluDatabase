package com.blu.database.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class SysDatabaseConsumer extends DatabaseConsumer {
    private static final String sClassName = SysDatabaseConsumer.class.getName();
    private static final Logger LOGGER = Logger.getLogger(sClassName);
    private static int COLUMN_NDX_TIME = 1;
    private static int COLUMN_NDX_BUILD_DATE_STRING = 4;
    private static int COLUMN_NDX_TIME_CONST_CUR = 15;
    private static int COLUMN_NDX_ZEROS1 = 17;
    private static int COLUMN_NDX_ZEROS2 = 18;
    private static int COLUMN_NDX_MFG_TEST_RESULT = 19;
    private static int COLUMN_MFG_TESTS_START = 20;
    private static int COLUMN_MFG_TESTS_END = 25;
    private static int COLUMN_NDX_MFG_TEST_ALL_PASSED = 27;

    private static int NUM_DATA_VALS_WITHOUT_MFG_TESTS = 20;
    private static int NUM_DATA_VALS_WITH_MFG_TESTS = 28;
    
    public SysDatabaseConsumer(String host, String dbName, String username, 
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
                        
            // consume messages until exit file is received
            while (true) {
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
                                new FileInputStream(curFileAbsPath), "UTF-8"))) {
                    // get the column keys
                    String keys = br.readLine();
                    if (keys == null || keys.isEmpty()) {
                        LOGGER.severe("Error processing keys: " + keys);
                        continue;
                    }
                    
                    String[] arrKeys = keys.split(",");
                    StringBuilder keyBldr = new StringBuilder();
                    for (int kNdx = 0; kNdx < arrKeys.length - 2; kNdx++) {
                        keyBldr.append("\"" + arrKeys[kNdx].trim() + "\",");
                    }
                    keyBldr.append("\"Machine Serial\",");
                    keyBldr.append("\"mfg_test_pack_detection\",");
                    keyBldr.append("\"mfg_test_firmware_version_valid\",");
                    keyBldr.append("\"mfg_test_serial_number_valid\",");
                    keyBldr.append("\"mfg_test_pack_volts\",");
                    keyBldr.append("\"mfg_test_cig_detect_volts\",");
                    keyBldr.append("\"mfg_test_cig_volts\",");
                    keyBldr.append("\"mfg_test_notes\",");
                    keyBldr.append("\"mfg_test_all_passed\",");
                    keyBldr.append("\"Log Filename\"");
                    
                    // read the file line by line                   
                    String curDataLine;
                    StringBuilder valBldr = new StringBuilder();
                    while ((curDataLine = br.readLine()) != null) {
                        String[] arrVals = curDataLine.split(",");
                        
                        // make sure there are the correct number of values
                        if (arrVals.length != NUM_DATA_VALS_WITHOUT_MFG_TESTS && 
                                arrVals.length != NUM_DATA_VALS_WITH_MFG_TESTS) {
                            LOGGER.severe("Error processing line: " + curDataLine);
                            continue;
                        }
                        
                        valBldr.setLength(0);
                        for (int valNdx = 0; valNdx < arrVals.length; valNdx++) {
                            String curVal = arrVals[valNdx];
                            if (valNdx == COLUMN_NDX_TIME) {
                                curVal = curVal.replaceAll("\u4e0a", "A");
                                curVal = curVal.replaceAll("\u4e0b", "P");
                                curVal = curVal.replaceAll("\u5348", "M");
                            }
                            
                            if (valNdx == COLUMN_NDX_BUILD_DATE_STRING) {
                                curVal = curVal.replace(" ", "");
                            }
                            
                            if (valNdx >= COLUMN_MFG_TESTS_START && 
                                    valNdx <= COLUMN_MFG_TESTS_END || 
                                    valNdx == COLUMN_NDX_MFG_TEST_ALL_PASSED) {
                                curVal = curVal.toLowerCase().contains("pass") ? 
                                            "TRUE" : "FALSE";
                            }
                            
                            if (valNdx != COLUMN_NDX_TIME_CONST_CUR && 
                                    valNdx != COLUMN_NDX_ZEROS1 && 
                                    valNdx != COLUMN_NDX_ZEROS2 && 
                                    valNdx != COLUMN_NDX_MFG_TEST_RESULT) {
                                if (curVal.isEmpty()) {
                                    valBldr.append("NULL,");
                                }
                                else {
                                    valBldr.append("'" + curVal.trim() + "',");
                                }
                            }
                            
                        }
                        if (arrVals.length == NUM_DATA_VALS_WITHOUT_MFG_TESTS) {
                            valBldr.append(
                                "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,");
                        }
                        valBldr.append("'" + curFilename + "'");
                        
                        // insert the data into the database
                        String query = 
                            String.format("INSERT INTO %s (%s) VALUES (%s)", 
                                          mTable, keyBldr.toString(), 
                                          valBldr.toString());
                        this.mConn.createStatement().executeUpdate(query);
                    }
                } catch (FileNotFoundException e) {
                    LOGGER.severe(e.toString());
                } catch (IOException e) {
                    LOGGER.severe(e.toString());
                }
            }
         } catch(InterruptedException e) {
             LOGGER.severe(e.toString());
         } catch (ClassNotFoundException | SQLException e) {
             LOGGER.severe(e.toString());
         }
         LOGGER.info("SYS Consumer finished.");
    }
    
    private static void dumpString(String text)
    {
        for (int i = 0; i < text.length(); i++) {
            System.out.println("U+" + Integer.toString(text.charAt(i), 16) + 
                                " " + text.charAt(i));
        }
    }
}

/*
CREATE TABLE public.sys_test_logs
(
  id SERIAL PRIMARY KEY,
  "Date" date,
  "Time" time without time zone,
  "Firmware Version" real,
  "Serial Number" integer,
  "Build Date String" character varying(32),
  "Board Revision" smallint,
  "Tester ID" smallint,
  "Calibration Value" real,
  "Pack Volts" real,
  "Pack Charging State" character varying(32),
  "Cig Volts" real,
  "Cig milliamps" real,
  "PWM Now" smallint,
  "Charge State" character varying(32),
  "Cig Detect Volts" real,
  "Machine Serial" character varying(32),
  mfg_test_pack_detection boolean,
  mfg_test_firmware_version_valid boolean,
  mfg_test_serial_number_valid boolean,
  mfg_test_pack_volts boolean,
  mfg_test_cig_detect_volts boolean,
  mfg_test_cig_volts boolean,
  mfg_test_notes character varying(64),
  mfg_test_all_passed boolean,
  "Log Filename" character varying(256)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.sys_test_logs
  OWNER TO postgres;
*/
