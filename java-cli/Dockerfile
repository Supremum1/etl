FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /src
COPY java-cli/pom.xml /src/pom.xml
COPY java-cli/src /src/src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /benchmark
COPY --from=build /src/target/oltp-olap-java-cli.jar /benchmark/oltp-olap-java-cli.jar

ENTRYPOINT ["java", "-jar", "/benchmark/oltp-olap-java-cli.jar"]
