# Avro schema manager for Apache Avro

The application allows you to generate Avro schemas right from RDBS 
and other sources which can produce CSV file containing params such as field name and data type.  

## Setup tips

- git clone <repository-link>

In the root directory of the project launch Maven build using command below:

- mvn clean install

Get a schema-validator-1.0-SNAPSHOT.uber.jar from generated /target 

Launch app in terminal choosing one of three available mods and provided database.properties file from src/main/resources directory
1) `-g` generates an Avro schemas if .CSV files provided 
2) `-v` verifies provided Avro schemas with provided .CSV producing verification .CSV file 
3) `gv` generates and verify Avro schemas using database information schemas and producing .CSV verification file  

```sh
java -jar <path-to-jar> <mode> <path-to-propetties> <coma-separeted-tables-to-genegate-of-verify>
```

EXAMPLE:  `java -jar schema-validator-1.0-SNAPSHOT.uber.jar -gv database.properties bet`


TODO: generate schemas straight from rabbit MQ
## Built With
* [Maven](https://maven.apache.org/) - Dependency Management

