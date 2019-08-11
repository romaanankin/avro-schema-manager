package com;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        AvroGenerator avroGenerator = new AvroGenerator();
        AvroVerificator avroVerificator = new AvroVerificator();
        AvroManager avroManager = new AvroManager(avroVerificator, avroGenerator);
        avroManager.manage(args);
    }
}
