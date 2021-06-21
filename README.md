## Project template for using DCM

A template for projects that want to get started with using
[DCM](https://github.com/vmware/declarative-cluster-management).

It contains a simple class named [App](src/main/java/com/vmware/App.java),
a [schema](src/main/resources/schema.sql) for the system's state, and 
a class for unit tests named [AppTest](src/test/java/com/vmware/AppTest.java).

We use [JOOQ](github.com/jOOQ/jOOQ) for convenience. The build is configured
with JOOQ's code-generator plugin. It generates type-safe Java classes to interact
with the [cluster state schema](src/main/resources/schema.sql).


To build this project:

```shell
$: mvn package
```