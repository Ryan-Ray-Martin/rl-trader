The docker compose file in this project contains a KSQLdb-CLI that runs properly on the http://ksqldb-server:8088

This means that the KSQLDB server is indeed running inside the docker container and not on the local machine. 

Compose this file in a headless state with:

$ docker-compose up -d

The KSQLDB-CLI can then be started with:

$ docker-compose exec ksqldb-cli ksql http://ksqldb-server:8088

Summary: 

This crypto trader folder contains the necessary applications to run crypto trading pipeline. The crypto_polygon.py script accesses the websocket to the polygon.io python-client, which retrieves the minute-by-minute aggregate bar data on selected cryptocurrencies. These messages from the websocket are produced to a kafka topic. 

As of now, the ksql.py script contains KSQLDB-CLI commands that will create a stream of data from the kafka topic, structure the streaming data into a useful table, and then write that streaming table into a topic that acts as a sliding window. 

The faust_crypto.py script handles the streaming data and serializes the topic into a json format with a dataclass object, similiar to asdict. 

# TODO:

The faust_crypto.py script needs to query the ray server to get the json response to send to the broker.

