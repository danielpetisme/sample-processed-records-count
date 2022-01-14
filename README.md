# Sample Processed Records Count

The purpose of this repository is to showcase how Kafka Streams counts the number of processed records.
The StreamThread is summing the records processed by each indiviual tasks.
Given the following topology:

```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [input])
      --> KSTREAM-MAPVALUES-0000000001
    Processor: KSTREAM-MAPVALUES-0000000001 (stores: [])
      --> my-repartition-filter
      <-- KSTREAM-SOURCE-0000000000
    Processor: my-repartition-filter (stores: [])
      --> my-repartition-sink
      <-- KSTREAM-MAPVALUES-0000000001
    Sink: my-repartition-sink (topic: my-repartition)
      <-- my-repartition-filter

  Sub-topology: 1
    Source: my-repartition-source (topics: [my-repartition])
      --> KSTREAM-MAPVALUES-0000000005
    Processor: KSTREAM-MAPVALUES-0000000005 (stores: [])
      --> KSTREAM-SINK-0000000006
      <-- my-repartition-source
    Sink: KSTREAM-SINK-0000000006 (topic: output)
      <-- KSTREAM-MAPVALUES-0000000005
```

One record sent in the input topic will be counted as
* 1 by the sub-topology 0
* 1 by the sub-topology 1
* 2 at the thread level

# Usage
```
./start.sh
``` 
