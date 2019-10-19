# Spark flow on top of the Wikipedia SSE Stream

## How-to run

- Create the docker network:
```bash
make create-network
```
- Run the streaming appliance
```bash
make run-appliance
```
- To run streaming consumption of data via legacy API (DStreams), please run:
```bash
make run-legacy-consumer
```

- To run streaming consumption of data via structured API, please run:
```bash
make run-structured-consumer
```

- To run streaming consumption of data via structured API with write to delta, please run:
```bash
make run-analytics-consumer
```

- Consumer reader from the folder in which Analytics Consumer writes, which will simply display it on the console, run:
```bash
make run-delta-read-consumer
```

You could also access the SparkUI for this Job at http://localhost:4040/jobs

```bash
docker-compose exec postgres psql -U consumer
\d
select * from types_count limit 10;
select * from types_count where load_dttm = (select max(load_dttm) from types_count);
select * from types_count where load_dttm = (select max(load_dttm) from types_count) order by count desc;
```

```bash
docker stats
```

## Known issues

- Sometimes you need to increase docker memory limit for your machine (for Mac it's 2.0GB by default).
- To debug memory usage and status of the containers, please use this command:
```bash
docker stats
```
- Sometimes docker couldn't gracefully stop the consuming applications, please use this command in case if container hangs:
```bash
docker-compose -f <name of compose file with the job>.yaml down
```
