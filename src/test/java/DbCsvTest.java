import com.betconstruct.config.AvroManagerConfig;
import com.betconstruct.config.DBUtil;
import com.betconstruct.config.JdbcManager;
import org.junit.Test;

import java.util.Properties;

public class DbCsvTest {
    private JdbcManager jdbcManager;
    private Properties properties;
    private String rawQuery;
    private String csvDirectory;
    private String verificationResultDirectory;
    private String avroDirectory;
    private String avroNameSpace;
    private String[] tables;

    @Test
    void dbFieldsCounterTest() {
//        properties = AvroManagerConfig.getProperties("/home/roman/Documents/schemavalidator/src/main/resources/database.properties");
//        jdbcManager = DBUtil.getJdbcManager(properties);
//
//        csvDirectory = properties.getProperty(AvroManagerConfig.DB_CSV_DIR);
//        verificationResultDirectory = properties.getProperty(AvroManagerConfig.VERIFICATION_RESULTS_DIR);
//        avroDirectory = properties.getProperty(AvroManagerConfig.AVRO_DIR);
//        avroNameSpace = properties.getProperty(AvroManagerConfig.AVRO_NAMESPACE);
//        rawQuery = properties.getProperty(AvroManagerConfig.QUERY);
//        tables = properties.getProperty(AvroManagerConfig.TABLE_NAME).split(",");

//        jdbcManager.writeCsv()
    }
}
