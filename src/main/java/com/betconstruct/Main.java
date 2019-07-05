package com.betconstruct;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        String outputAvro = "/home/roman/Documents/schemavalidator/src/avro/generatedAvro.avsc";
        String resultkFilePAth = "/home/roman/Documents/schemavalidator/src/main/resources/verificationResult.csv";
        String CSVSource = "/home/roman/Documents/schemavalidator/src/main/resources/dbCsv/client.csv";

        new AvroGenerator().writeAvroFile(CSVSource,outputAvro,"test1","Avro");

        new AvroVerification().validateSchema(resultkFilePAth,outputAvro,CSVSource);
    }
}
