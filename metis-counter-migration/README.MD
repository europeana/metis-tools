# metis-counter-migration

This project is a Spring Batch utility used to validate and repair Metis workflow counters in MongoDB.

## What it does

The application reads dataset and workflow execution data from Metis Core, checks each executable plugin against the progress reported by the connected engine task, and updates the stored Metis counters when they no longer match the real execution state.

In practice, it does the following:

- loads datasets from the Metis Core Mongo database
- retrieves all workflow executions for each dataset
- inspects executable plugins and their stored `ExecutionProgress` values
- queries the related engine task progress from the external task service
- compares the counters (expected, processed, success, fail, warning, duplicate, unchanged, and de-publish counts)
- writes back the corrected values when Metis and eCloud diverge

This is intended for migration and reconciliation scenarios where workflow counters drifted or became outdated after reprocessing.

## Why this exists

Metis stores execution progress as part of each workflow execution. Over time, those counters may become inconsistent with the actual engine task results. This tool reconciles the values so the Metis database reflects the live workflow state.

It does not reindex records or re-run full dataset processing by itself; it focuses on checking and fixing the execution counters.

## Main configuration

Copy the example configuration before running the project:

- `src/main/resources/application.properties.example` to `src/main/resources/application.properties`

The most important settings are:

- `mode` — `PRE_PROCESS`, `DEFAULT`, `REPROCESS_ALL_FAILED` or `POST_PROCESS`
- `dataset.ids.to.process` — optional list of dataset IDs to process
- `min.parallel.datasets` and `max.parallel.threads.per.dataset` — batch parallelism settings
- MongoDB connection details for Metis Core
- truststore options, if required by the environment
- engine/eCloud configuration used to fetch task statistics

## Supported modes

- `PRE_PROCESS` — inspect mismatches without writing changes
- `DEFAULT` — preview and apply corrections when differences are found
- `REPROCESS_ALL_FAILED` — focused repair mode for failed counters
- `POST_PROCESS` — final verification or post-migration check

## Build and run

Build the project:

```bash
mvn clean package
```

Run the application:

```bash
java -jar target/metis-counter-migration-1.0-SNAPSHOT.jar
```

You can also run it directly from the IDE using the Spring Boot entry point in `MigrationCounterMain`.

## Project structure

- `src/main/java` — application code
- `src/main/resources/application.properties.example` — sample configuration
- `src/main/resources/log4j2.xml` — logging configuration
- `src/test/java` — test sources

## Notes

This project is built around Spring Batch and MongoDB, and it is designed for operational data reconciliation rather than regular record processing.

