ADD JAR '/opt/job/flink-job.jar';
SET 'execution.target' = 'remote';
SET 'rest.address' = 'jobmanager';
SET 'table.dml-sync' = 'true';
SET 'pipeline.name' = 'local-stack-verification';

CREATE TABLE verification_input (payload STRING) WITH (
    'connector' = 'kafka',
    'topic' = 'stack-check-in',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'stack-check',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'format' = 'raw'
);

CREATE TABLE verification_output (payload STRING) WITH (
    'connector' = 'kafka',
    'topic' = 'stack-check-out',
    'properties.bootstrap.servers' = 'kafka:9092',
    'sink.delivery-guarantee' = 'at-least-once',
    'format' = 'raw'
);

INSERT INTO verification_output SELECT payload FROM verification_input;
