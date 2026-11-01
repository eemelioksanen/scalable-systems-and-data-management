FROM docker.io/library/eclipse-temurin:17-jre
COPY producer/target/producer.jar /app/producer.jar
ENTRYPOINT ["java", "-Xmx512m", "-cp", "/app/producer.jar"]
