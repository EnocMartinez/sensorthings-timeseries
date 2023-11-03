# SensorThings TimeSeries #


SensorThings TimeSeries is an API that is designed to complement the SensorThings API ([FROST Implementation](https://github.com/FraunhoferIOSB/FROST-Server)) with efficient storage for large amounts of time-series data with [TimescaleDB](https://timescaledb.com). SensorThings TimeSeries provides an API to access to data stored in a TimescaleDB hypertable called `raw_data` and returns it to the users.

The SensorThings TimeSeries API provides transparent access to SensorThings data, while adding support to access huge amount of timeseries data. This approach has the benefits of saving lots of hard drive space and to speed up queries retrieving great amounts of data.

```
                ┌──────────────┐                   ┌──────────────────┐
                │              │                   │                  │
      o   ──────► SensorThings ├───────────────────► SensorThings API │
     -|-        │  TimeSeires  │   regular queries │  (FROST Server)  │
     / \        │              ├───┐   (http)      │                  │
                └──────────────┘   │               └──────────────────┘
   (user)                          │
                                   │
                                   │               ┌──────────────────┐
                                   │               │ SensorThings DB  │
                                   └───────────────► (PostgresQL +    │
                                   queries with    │  TimescaleDB)    │
                                   timeseries data └──────────────────┘
                                   (postgresq-)


```


### Info ###

* **author**: Enoc Martínez
* **version**: v0.2.1
* **contributors**: Enoc Martínez 
* **organization**: Universitat Politècnica de Catalunya (UPC)
* **contact**: enoc.martinez@upc.edu