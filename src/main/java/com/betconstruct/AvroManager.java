package com.betconstruct;

import com.betconstruct.config.AvroManagerConfig;
import com.betconstruct.config.DBUtil;
import com.betconstruct.config.JdbcManager;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.Properties;

public class AvroManager {
    private AvroGenerator avroGenerator;
    private AvroVerificator avroVerificator;
    public static final String CSV_FILE_FORMAT = ".csv";
    public static final String AVRO_FILE_FORMAT = ".avsc";
    private Properties properties;
    private String rawQuery;
    private String csvDirectory;
    private String verificationResultDirectory;
    private String avroDirectory;
    private String avroNameSpace;
    private String mode;
    private String configPath;
    private String[] tables;

    public AvroManager(AvroVerificator avroVerificator, AvroGenerator avroGenerator) {
        this.avroVerificator = avroVerificator;
        this.avroGenerator = avroGenerator;
    }

    public void manage(String[] args) throws IOException {

        String infoMessage = "Available mods:\n " +
                "<mode> <path-to-propetties> <coma-separeted-tables-to-genegate-of-verify>\n" +
                "1) `-g` generates an Avro schemas if .CSV files provided \n" +
                "2) `-v` verifies provided Avro schemas with provided .CSV producing verification .CSV file \n" +
                "3) `gv` generates and verify Avro schemas using database information schemas and producing .CSV verification file ";

        if(args.length == 1 && args[0].equals("-h")) {
            System.out.println(infoMessage);
            return;
        }
        configure(args);
        if(args.length > 2) {
            tables = args[2].split(",");
        }

        switch (mode) {
            case "-g":
                generateAvroFromDbInfo(tables,rawQuery,properties,csvDirectory,avroDirectory,avroNameSpace);
                break;
            case "-v" :
                validateAvroSchemas(properties,rawQuery,avroDirectory,verificationResultDirectory,csvDirectory);
                break;
            case "-gv":
                generateAvroFromDbInfo(tables,rawQuery,properties,csvDirectory,avroDirectory,avroNameSpace);
                validateAvroSchemas(properties,rawQuery,avroDirectory,verificationResultDirectory,csvDirectory);
                break;
            default:
                System.out.println(infoMessage);
        }
    }

    private void configure(String[] args) {
        mode = args[0];
        configPath = args[1];
        properties = AvroManagerConfig.getProperties(configPath);
        csvDirectory = properties.getProperty(AvroManagerConfig.DB_CSV_DIR);
        verificationResultDirectory = properties.getProperty(AvroManagerConfig.VERIFICATION_RESULTS_DIR);
        avroDirectory = properties.getProperty(AvroManagerConfig.AVRO_DIR);
        avroNameSpace = properties.getProperty(AvroManagerConfig.AVRO_NAMESPACE);
        rawQuery = properties.getProperty(AvroManagerConfig.QUERY);
        tables = properties.getProperty(AvroManagerConfig.TABLE_NAME).split(",");
    }

    private void generateAvroFromDbInfo(String[] tables, String rawQuery, Properties properties, String csvDirectory,
                                        String avroDirectory, String avroNameSpace) throws IOException {
        for (String table : tables) {
            writeCSVfromDb(properties,table,rawQuery,csvDirectory);
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
                String avroOutput = avroDirectory + name + AVRO_FILE_FORMAT;

             avroGenerator.writeAvroFile(canonicalPath,avroOutput, avroNameSpace, StringUtils.capitalize(name));
            }
        }
    }

    private void validateAvroSchemas(Properties properties, String rawQuery, String avroDirectory, String verificationResultDirectory,
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
                writeCSVfromDb(properties,name,rawQuery,csvDirectory);
                String inputFileName = verificationResultDirectory + name + "-verification" + CSV_FILE_FORMAT;
                String outputFileName = csvDirectory + name + CSV_FILE_FORMAT;

                avroVerificator.validateSchema(inputFileName,canonicalPath,outputFileName);
            }
        }
    }

    private void writeCSVfromDb(Properties properties, String table,String rawQuery,String csvDirectory) throws FileNotFoundException {
        JdbcManager jdbcManager = DBUtil.getJdbcManager(properties);
        String query = String.format(rawQuery, table);
        File file = new File(csvDirectory);
        if (!file.exists()) {
            file.mkdir();
        }
        PrintStream printStream = new PrintStream(new File(String.format(csvDirectory + "/%s" + CSV_FILE_FORMAT, table)));
        try {
            jdbcManager.writeCsv(query, printStream);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
