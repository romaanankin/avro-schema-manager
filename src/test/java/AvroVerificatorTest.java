import com.betconstruct.AvroVerificator;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroVerificatorTest {
    private AvroVerificator app;
    private String resultFilePath;
    private String sourceFileAvro;
    private String CSVSource;

    @Test
    public void verificatorIOE() {
        resultFilePath = "";
        sourceFileAvro = "";
        CSVSource = "";
        app = new AvroVerificator();
        try {
            app.validateSchema(resultFilePath,sourceFileAvro,CSVSource);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    @Test
    public void fileVerificationTest() {
        String verificationEthalon = "src/test/resources/verification.result/verification-ethalon.csv";
        resultFilePath = "src/test/resources/verification.result/verification-result.csv";
        sourceFileAvro = "src/test/resources/avro/test.avsc";
        CSVSource = "src/test/resources/data.base.csv/test.csv";
        app = new AvroVerificator();
        try {
            app.validateSchema(resultFilePath,sourceFileAvro,CSVSource);
        } catch (Exception e) {
            e.printStackTrace();
        }
        File file1 = new File(verificationEthalon);
        File file2 = new File(resultFilePath);
        try {
         boolean isTwoEqual = FileUtils.contentEquals(file1, file2);
         Assert.assertTrue(isTwoEqual);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
