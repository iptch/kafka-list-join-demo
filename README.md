# Kafka List Join Demo

This project shows a way to perform a list join in a Kafka streams application. A list join refers to joining a record
that contains a list with a KTable, such that each element in the list gets joined with the corresponding element in the
KTable.

## Usage

To build and test the project, run
```shell
./gradlew clean build
```
