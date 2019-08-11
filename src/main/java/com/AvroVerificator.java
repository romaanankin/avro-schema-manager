package com;

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

public class AvroVerificator {
    public void validateSchema(String resultFilePath, String sourceFileAvro, String CSVSourse) throws IOException {
        Map<String, Type>  dataTypesInBase = CSVToMap(CSVSourse);
        JSONObject avroJson = parseJSONFile(sourceFileAvro);
        Map<String, Type> dataTypesInAvro = avroToMap(avroJson);

        BufferedWriter writer = Files.newBufferedWriter(Paths.get(resultFilePath));

        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                .withHeader("Field", "FormatInBase", "FormatInAvro"));

        int counter = 0;
        for (Map.Entry<String, Type> inBase : dataTypesInBase.entrySet()) {
            String key = inBase.getKey();
            Type type = dataTypesInAvro.get(key);
            if (type != null) {
                csvPrinter.printRecord(key, inBase.getValue().getDataType(), type.getDataType());
                System.out.println("Printing types from database to verification CSV :" + counter++);
            } else {
                csvPrinter.printRecord(key, inBase.getValue().getDataType(), "");
                System.out.println("Printing types from database to verification CSV :" + counter++);

            }
        }
        counter = 0;
        for (Map.Entry<String, Type> inAvro : dataTypesInAvro.entrySet()) {
            String key = inAvro.getKey();
            Type type = dataTypesInBase.get(key);
            if (type == null) {
                csvPrinter.printRecord(key, "", inAvro.getValue().getDataType());
                System.out.println("Printing types from Avro file to verification CSV :" + counter++);
            }
        }
        csvPrinter.flush();
        csvPrinter.close();
        System.out.println("Verification file READY");
    }

    private JSONObject parseJSONFile(String filename) throws JSONException, IOException {
        String content = new String(Files.readAllBytes(Paths.get(filename)));
        return new JSONObject(content);
    }

    private Map<String, Type> CSVToMap(String CSVsource) throws IOException {
        HashMap<String, Type> stringTypeHashMap = new HashMap<>();
        Reader in = new FileReader(CSVsource);
        CSVParser csvParser = new CSVParser(in, CSVFormat.DEFAULT.withHeader());

        for (CSVRecord record : csvParser) {
            Type type = new Type();
            type.setDataType(record.get(1));
            type.setNumericPrecision(record.get(2));
            type.setNumericScale(record.get(3));
            stringTypeHashMap.put(record.get(0), type);
        }
        return stringTypeHashMap;
    }

    private Map<String, Type> avroToMap(JSONObject jsonObject){
        HashMap<String, Type> stringTypeHashMap = new HashMap<>();
        JSONArray fields = jsonObject.getJSONArray("fields");
        for (int i = 0; i < fields.length(); i++) {
            JSONObject jsonObject1 = fields.getJSONObject(i);
            Type type = new Type();
            try {
                type.setDataType(jsonObject1.getJSONArray("type").getString(1));
            } catch (JSONException e) {
                type.setDataType(jsonObject1.getJSONArray("type").getJSONObject(1).toString());
            }
            stringTypeHashMap.put(jsonObject1.getString("name"), type);
        }
        return stringTypeHashMap;
    }
}
