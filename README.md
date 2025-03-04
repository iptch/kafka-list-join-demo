# Kafka List Join Demo

This project shows two ways to perform a list join in a Kafka streams application. A list join refers to joining a
record that contains a list with a KTable, such that each element in the list gets joined with the corresponding element
in the KTable.

This image shows the high level idea, joining a persons address list, with the corresponding addresses:

![List join overview](/ListJoin.png)

Check out `TODO: add link` for a discussion of the approaches.

## Building

To build and test the project, run
```shell
./gradlew clean build
```
