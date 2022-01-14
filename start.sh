#!/bin/bash

docker-compose -f ./docker-compose.yml up -d

# Verify Confluent Control Center has started within MAX_WAIT seconds
MAX_WAIT=480
CUR_WAIT=0
echo "Waiting up to $MAX_WAIT seconds for Confluent Control Center control-center to start"
docker container logs control-center > /tmp/out.txt 2>&1
while [[ ! $(cat /tmp/out.txt) =~ "Started NetworkTrafficServerConnector" ]]; do
  sleep 10
  docker container logs control-center > /tmp/out.txt 2>&1
  CUR_WAIT=$(( CUR_WAIT+10 ))
  if [[ "$CUR_WAIT" -gt "$MAX_WAIT" ]]; then
    echo -e "\nERROR: The logs in control-center container do not show 'Started NetworkTrafficServerConnector' after $MAX_WAIT seconds. Please troubleshoot with 'docker container ps' and 'docker container logs'.\n"
    exit 1
  fi
done
echo "Control Center has started!"

echo "Producing one record"
echo "1,101" | docker exec -i broker kafka-console-producer --bootstrap-server broker:9092 --topic input --property "parse.key=true" --property "key.separator=,"
sleep 10
echo "Consuming one record"
docker exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic output --from-beginning --property "print.key=true" --property "key.separator=," --max-messages 1

echo "Thread: 1 record sent to 2 sub-topologies equals 2 processed record"
curl -s  http://localhost:8081/metrics | grep kafka_stream_thread_process_total | grep -v "#"

echo "Task: Each task (ie. sub-topologies) processed 1 record"
curl -s  http://localhost:8081/metrics | grep kafka_stream_task_process_total | grep -v "#"