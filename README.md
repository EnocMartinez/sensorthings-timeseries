# SensorThings TimeSeries #


SensorThings TimeSeries is an API that is designed to complement the SensorThings API ([FROST Implementation](https://github.com/FraunhoferIOSB/FROST-Server)) with efficient storage for large amounts of time-series data with [TimescaleDB](https://timescaledb.com). SensorThings TimeSeries provides an API to access to data stored in several TimescaleDB hypertable called `timeseries`, `profiles` and `detections` and returns it to the users.

The SensorThings TimeSeries API provides transparent access to SensorThings data, while adding support to access huge amount of timeseries data. This approach has the benefits of saving lots of hard drive space and to speed up queries retrieving great amounts of data.

```


            ┌──────────────┐  regular queries  ┌──────────────────┐
            │              │      (http)       │                  │
  o   ──────► SensorThings ├───────────────────► SensorThings API │
 -|-        │  TimeSeries  │                   │  (FROST Server)  │
 / \        │              │                   │                  │
            └──────┬───────┘                   └─────────┬────────┘
user)              │                                     │
                   │                                     │ (PG connection)
                   │                                     │
                   │                           ┌─────────O────────┐
                   │     (PG connection)       │ SensorThings DB  │
                   └───────────────────────────O (PostgresQL +    │
                                               │  TimescaleDB)    │
                          queries with         └──────────────────┘
                          timeseries data
                          (postgresql)


```

Currently, there are 3 types of data:
* **timeseries**: regular timeseries data. Each point has a timestamp and a quality control flag associated
* **profiles**: depth-referenced data. Each data point has a timestamp, a depth (int) and a quality control flag associated
* **detections**: class predictions from an AI-based object detection algorithm. Each data point is an integer value (number of instances detected) with an associated timestamp.

### Info ###

* **author**: Enoc Martínez
* **version**: v0.2.1
* **contributors**: Enoc Martínez 
* **organization**: Universitat Politècnica de Catalunya (UPC)
* **contact**: enoc.martinez@upc.edu