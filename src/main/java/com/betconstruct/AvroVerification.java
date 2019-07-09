package com.betconstruct;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class AvroVerification {
    void validateSchema(String resultFilePath, String sourceFileAvro, String CSVSourse) throws IOException {

        Map<String, Type> dataTypesInBase = new HashMap<>();
        Map<String, Type> dataTypesInAvro = new HashMap<>();


        JSONObject jsonObject = parseJSONFile(sourceFileAvro);

        Reader in = new FileReader(CSVSourse);

        CSVParser csvParser = new CSVParser(in, CSVFormat.DEFAULT.withHeader());

        BufferedWriter writer = Files.newBufferedWriter(Paths.get(resultFilePath));

        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader("Field", "FormatInBase", "FormatInAvro"));

        for (CSVRecord strings : csvParser) {
            Type type = new Type();
            type.setDataType(strings.get(1));
            type.setNumericPrecision(strings.get(2));
            type.setNumericScale(strings.get(3));
            dataTypesInBase.put(strings.get(0), type);
        }

        JSONArray fields = jsonObject.getJSONArray("fields");
        for (int i = 0; i < fields.length(); i++) {
            JSONObject jsonObject1 = fields.getJSONObject(i);
            Type type = new Type();
            try {
                type.setDataType(jsonObject1.getJSONArray("type").getString(1));
            } catch (JSONException e) {
                type.setDataType(jsonObject1.getJSONArray("type").getJSONObject(1).toString());
            }
            dataTypesInAvro.put(jsonObject1.getString("name"), type);
        }

        for (Map.Entry<String, Type> inBase : dataTypesInBase.entrySet()) {
            String key = inBase.getKey();
            Type type = dataTypesInAvro.get(key);
            if (type != null) {
                System.out.println(key + type.getDataType() + inBase.getValue().getDataType());
                csvPrinter.printRecord(key, inBase.getValue().getDataType(), type.getDataType());
            } else {
                csvPrinter.printRecord(key, inBase.getValue().getDataType(), "");
            }
        }

        for (Map.Entry<String, Type> inAvro : dataTypesInAvro.entrySet()) {
            String key = inAvro.getKey();
            Type type = dataTypesInBase.get(key);
            if (type == null) {
                csvPrinter.printRecord(key, "", inAvro.getValue().getDataType(), "true");
            }
        }
        csvPrinter.flush();
    }

    private JSONObject parseJSONFile(String filename) throws JSONException, IOException {
        String content = new String(Files.readAllBytes(Paths.get(filename)));
        return new JSONObject(content);
    }
}
