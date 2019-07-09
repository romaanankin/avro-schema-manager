package com.betconstruct;

import com.betconstruct.config.DBUtil;
import com.betconstruct.config.JdbcManager;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

public class AvroManager {
    private final static String AVRO_NAMESPACE = "avro.namespace";
    private final static String DB_CSV_DIR = "db.csv.dir";
    private final static String VERIFICATION_RESULTS_DIR = "verification.result.dir";
    private final static String AVRO_DIR = "avro.dir";
    private final static String TABLE_NAME = "table.name";
    private final static String QUERY = "query";
    public final static String DB_URL = "url";
    public final static String USER = "user";
    public final static String PASSWORD = "password";
    public final static String DB_NAME = "data.base.name";
    private String rawQuery;
    private String csvDirectory;
    private String verificationResultDirectory;
    private String avroDirectory;
    private String avroNameSpace;

    public void manage(String[] args) throws IOException {
        Properties properties = new Properties();

        String mode = args[0];
        String configPath = args[1];
        String[] tables = args[2].split(",");

        try (InputStream inputStream = new FileInputStream(configPath)) {
            properties.load(inputStream);
            csvDirectory = properties.getProperty(DB_CSV_DIR);
            verificationResultDirectory = properties.getProperty(VERIFICATION_RESULTS_DIR);
            avroDirectory = properties.getProperty(AVRO_DIR);
            avroNameSpace = properties.getProperty(AVRO_NAMESPACE);
            rawQuery = properties.getProperty(QUERY);
        } catch (IOException e) {
            e.printStackTrace();
        }

        switch (mode) {
            case "-g":
                generateAvroFromDbInfo(tables,rawQuery,properties,csvDirectory,avroDirectory,avroNameSpace);
                break;
            case "-v" :
                validateAvroSchemas(avroDirectory,verificationResultDirectory,csvDirectory);
                break;
            case "-gv":
                generateAvroFromDbInfo(tables,rawQuery,properties,csvDirectory,avroDirectory,avroNameSpace);
                validateAvroSchemas(avroDirectory,verificationResultDirectory,csvDirectory);
                break;
        }
    }

    private void generateAvroFromDbInfo(String[] tables, String rawQuery, Properties properties, String csvDirectory,
                                        String avroDirectory, String avroNameSpace) throws IOException {
        for (String table : tables) {
            JdbcManager jdbcManager = DBUtil.getJdbcManager(properties);
            String query = String.format(rawQuery, table);
            File file = new File(csvDirectory);
            if (!file.exists()) {
                file.mkdir();
            }
            PrintStream printStream = new PrintStream(new File(String.format(csvDirectory + "/%s.csv", table)));
            try {
                jdbcManager.writeCsv(query, printStream);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        File dbCsvDir = new File(csvDirectory);
        File[] csvFiles = dbCsvDir.listFiles();

        if (csvFiles!=null) {
            File file1 = new File(avroDirectory);
            if (!file1.exists()) {
                file1.mkdir();
            }
            for (File file : csvFiles) {
                String canonicalPath = file.getCanonicalPath();
                String fileName = file.getName();
                String name = fileName.substring(0, fileName.length() - 4);
                String avroOutput = avroDirectory + name + ".avsc";

                new AvroGenerator().writeAvroFile(canonicalPath,avroOutput, avroNameSpace, StringUtils.capitalize(name));
            }
        }
    }

    private void validateAvroSchemas(String avroDirectory, String verificationResultDirectory,
                                     String csvDirectory) throws IOException {
        File avroDir = new File(avroDirectory);
        if(!avroDir.exists()) {
            avroDir.mkdir();
        }
        File[] avroFiles = avroDir.listFiles();

        if (avroFiles!=null) {
            File verDir = new File(verificationResultDirectory);
            if(!verDir.exists()) {
                verDir.mkdir();
            }
            for (File file : avroFiles) {
                String canonicalPath = file.getCanonicalPath();
                String name = file.getName().substring(0, file.getName().length() - 5);
                String inputFileName = verificationResultDirectory + name + "-verification.csv";
                String outputFileName = csvDirectory + name + ".csv";

                new AvroVerification().validateSchema(inputFileName,canonicalPath,outputFileName);
            }
        }
    }
}
