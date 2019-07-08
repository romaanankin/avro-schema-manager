package com.betconstruct;

import com.betconstruct.config.DBUtil;
import com.betconstruct.config.JdbcManager;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

public class Main {
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


    public static void main(String[] args) throws IOException, SQLException {
        Properties properties = new Properties();
        String rawQuery = "";
        String csvDirectory = "";
        String verificationResultDirectory = "";
        String avroDirectory = "";
        String avroNameSpace = "";

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
            case "-generate":
                generateAvroFromDbInfo(tables,rawQuery,properties,csvDirectory,avroDirectory,avroNameSpace);
                break;
            case "-verify" :
                validateAvroSchemas(avroDirectory,verificationResultDirectory,csvDirectory);
        }

    }

    static void generateAvroFromDbInfo(String[] tables,String rawQuery,Properties properties, String csvDirectory,
                                String avroDirectory,String avroNameSpace) throws IOException, SQLException {
        for (String table : tables) {
            JdbcManager jdbcManager = DBUtil.getJdbcManager(properties);
            String query = String.format(rawQuery, table);
            PrintStream printStream = new PrintStream(new File(String.format(csvDirectory + "/%s.csv", table)));
            jdbcManager.writeCsv(query, printStream);
        }

        File dbCsvDir = new File(csvDirectory);
        File[] csvFiles = dbCsvDir.listFiles();

        if (csvFiles!=null) {
            for (File file : csvFiles) {
                String canonicalPath = file.getCanonicalPath();
                String fileName = file.getName();
                String name = fileName.substring(0, fileName.length() - 4);
                String avroOutput = avroDirectory + name + ".avsc";

                new AvroGenerator().writeAvroFile(canonicalPath,avroOutput, avroNameSpace, StringUtils.capitalize(name));
            }
        }
    }

    static void validateAvroSchemas(String avroDirectory, String verificationResultDirectory,
                                    String csvDirectory) throws IOException {
        File avroDir = new File(avroDirectory);
        File[] avroFiles = avroDir.listFiles();

        if (avroFiles!=null) {
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
