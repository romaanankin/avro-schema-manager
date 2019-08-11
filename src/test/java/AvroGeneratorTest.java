import com.AvroGenerator;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroGeneratorTest {
    private AvroGenerator app;
    private String inputFile;
    private String outputFile;
    private String namespace;
    private String avroObjectName;

    @Test
    public void generatorIOExTest() {
        inputFile = "";
        outputFile = "";
        namespace = "";
        avroObjectName = "";

        app = new AvroGenerator();
        try {
            app.writeAvroFile(inputFile,outputFile,namespace,avroObjectName);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IOException);
        }
    }

    @Test
    public void generatorTest() {
            String verificationEthalon = "src/test/resources/avro/test.avsc";
            outputFile = "src/test/resources/avro/renerated-result.avsc";
            inputFile = "src/test/resources/data.base.csv/test.csv";
            namespace = "namespace";
            avroObjectName = "Test";
            app = new AvroGenerator();
            try {
                app.writeAvroFile(inputFile,outputFile,namespace,avroObjectName);
            } catch (Exception e) {
                e.printStackTrace();
            }
            File file1 = new File(verificationEthalon);
            File file2 = new File(outputFile);
            try {
                boolean isTwoEqual = FileUtils.contentEquals(file1, file2);
                Assert.assertTrue(isTwoEqual);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
}
