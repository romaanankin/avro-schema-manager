package com;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class AvroGenerator {
    public void writeAvroFile(String inputFile, String outputFile, String namespace, String avroObjectName) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        writer.write(getAvroFromCsv(inputFile, namespace, avroObjectName));
        writer.close();
    }

    private String getAvroFromCsv(String inputFile, String namespace, String avroObjectName) throws IOException {
        Reader in = new FileReader(inputFile);
        CSVParser csvParser = new CSVParser(in,CSVFormat.DEFAULT.withHeader());
        List<CSVRecord> csvRecordList = new ArrayList<>();
        for (CSVRecord records: csvParser) {
            csvRecordList.add(records);
        }

        StringBuilder stringBuilder = new StringBuilder();

        for(int i = 0; i < csvRecordList.size(); i++) {
            System.out.println("Parsing CSV input fields : " + i);
            CSVRecord csvRecord = csvRecordList.get(i);
            String columnName = csvRecord.get(0);
            String dataType = convertTypes(csvRecord.get(1));
            String rawAvroField = String.format("    {\n" +
                    "      \"name\": \"%s\",\n" +
                    "      \"type\": [\n" +
                    "        \"null\",\n" +
                    "        %s\n" +
                    "      ],\n" +
                    "      \"default\": null\n" +
                    "    },\n",columnName,dataType);

            stringBuilder.append(rawAvroField);
            if(i==csvRecordList.size() - 1){
                stringBuilder.deleteCharAt(stringBuilder.length() - 2);
            }
        }

        String fields = String.valueOf(stringBuilder);
        return String.format("{\n" +
                "  \"namespace\": \"%s\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"%s\",\n" +
                "  \"fields\": [" +
                fields +
                "]\n}",namespace,avroObjectName);
    }

    private String convertTypes(String rawType) {
        String timestamp = "{\n" +
                "          \"type\": \"long\",\n" +
                "          \"connect.version\": 1,\n" +
                "          \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "        }";
        String time = "{\n" +
                "          \"type\": \"int\",\n" +
                "          \"connect.version\": 1,\n" +
                "          \"connect.name\": \"org.apache.kafka.connect.data.Time\"\n" +
                "        }";
        if (rawType.contains("bigint"))  return "\"long\"";
        if (rawType.contains("int"))  return "\"int\"";
        if (rawType.contains("decimal"))  return "\"double\"";
        if (rawType.contains("char"))  return "\"string\"";
        if (rawType.contains("bit"))  return "\"boolean\"";
        if (rawType.contains("timestamp"))  return "\"string\"";
        if (rawType.contains("date"))  return timestamp;
        if (rawType.equals("time"))  return time;
        else return rawType + "To Define";
    }
}
