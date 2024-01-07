FROM maven:3.8.6-openjdk-11-slim AS MAVEN_BUILD
COPY ./ ./
RUN mvn clean install -f ./aixm-5.1/pom.xml \
        && mvn clean install -f ./jms-client/pom.xml \
        && mvn clean install -f ./swim-utilities/pom.xml \
        && mvn clean package

FROM azul/zulu-openjdk-alpine:11

USER nobody

WORKDIR /app

COPY --chown=nobody:nobody --from=MAVEN_BUILD target/fns-client ./

ENTRYPOINT ["java", "-jar", "FnsClient.jar"]
