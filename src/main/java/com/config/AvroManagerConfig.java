package com.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AvroManagerConfig {
    public final static String AVRO_NAMESPACE = "avro.namespace";
    public final static String DB_CSV_DIR = "db.csv.dir";
    public final static String VERIFICATION_RESULTS_DIR = "verification.result.dir";
    public final static String AVRO_DIR = "avro.dir";
    public final static String TABLE_NAME = "table.name";
    public final static String QUERY = "query";
    public final static String DB_URL = "url";
    public final static String USER = "user";
    public final static String PASSWORD = "password";
    public final static String DB_NAME = "data.base.name";

    public static Properties getProperties(String path) {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(path)) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
