package com.betconstruct;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {

        String inputCsv = "/home/roman/Documents/schemavalidator/src/main/resources/client.csv";
        String outputAvro = "/home/roman/Documents/schemavalidator/src/avro/test.avsc";
        writeAvroFile(inputCsv,outputAvro,"test1","Avro");
    }

    static void writeAvroFile(String inputFile, String outputFile,String namespace,String avroObjectName) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
        writer.write(getAvroFromCsv(inputFile,namespace,avroObjectName));
        writer.close();
    }

    public static String getAvroFromCsv (String inputFile,String namespace,String avroObjectName) throws IOException {
        File file = new File(inputFile);
        Reader in = new FileReader(file);
        CSVParser csvParser = new CSVParser(in,CSVFormat.DEFAULT.withHeader());
        List<CSVRecord> csvRecordList = new ArrayList<>();
        for (CSVRecord records: csvParser) {
            csvRecordList.add(records);
        }

        StringBuilder stringBuilder = new StringBuilder();

        for(int i = 0; i < csvRecordList.size(); i++) {
            CSVRecord csvRecord = csvRecordList.get(i);
            String columnName = csvRecord.get(0);
            String dataType = convertTypes(csvRecord.get(1));
            String s1 = String.format("    {\n" +
                    "      \"name\": \"%s\",\n" +
                    "      \"type\": [\n" +
                    "        \"null\",\n" +
                    "        %s\n" +
                    "      ],\n" +
                    "      \"default\": null\n" +
                    "    },\n",columnName,dataType);

            stringBuilder.append(s1);
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

    public static String convertTypes(String rawType) {
        String timestamp = "{\n" +
                "          \"type\": \"long\",\n" +
                "          \"connect.version\": 1,\n" +
                "          \"connect.name\": \"org.apache.kafka.connect.data.Timestamp\"\n" +
                "        }";
        if (rawType.contains("bigint"))  return "\"long\"";
        if (rawType.equals("int"))  return "\"int\"";
        if (rawType.contains("decimal"))  return "\"double\"";
        if (rawType.contains("char"))  return "\"string\"";
        if (rawType.contains("bit"))  return "\"boolean\"";
        if (rawType.contains("timestamp"))  return "\"string\"";
        if (rawType.contains("date"))  return timestamp;
        else return rawType + "To Define";
    }
}
