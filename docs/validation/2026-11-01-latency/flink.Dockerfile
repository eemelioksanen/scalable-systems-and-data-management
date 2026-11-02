FROM docker.io/library/flink:2.2.1-java17
COPY flink-job/target/flink-job.jar /opt/job/flink-job.jar
