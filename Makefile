brokers:
	docker-compose up kafka-a zookeeper-a kafka-b zookeeper-b

build:
	mvn package

clean:
	docker-compose stop
	docker-compose rm -f
	rm -r -f ./target

mm:
	docker-compose up mm-1 mm-2

producer-a:
	docker run -i -t --network=kafkaproxy_default \
		confluentinc/cp-kafka:5.0.0-2 \
		kafka-console-producer --broker-list kafka-a:9092 --topic monolog-aggregate-2018-11-24

consumer-a:
	docker run -i -t --network=kafkaproxy_default \
		confluentinc/cp-kafka:5.0.0-2 \
		kafka-console-consumer --bootstrap-server kafka-a:9092 --topic monolog-aggregate-2018-11-24

producer-b:
	docker run -i -t --network=kafkaproxy_default \
		confluentinc/cp-kafka:5.0.0-2 \
		kafka-console-producer --broker-list kafka-b:9094 --topic monolog-aggregate-2018-11-24

consumer-b:
	docker run -i -t --network=kafkaproxy_default \
		confluentinc/cp-kafka:5.0.0-2 \
		kafka-console-consumer --bootstrap-server kafka-b:9094 --topic monolog-aggregate-2018-11-24

proxy-producer:
	docker run -i -t --network=kafkaproxy_default \
		confluentinc/cp-kafka:5.0.0-2 \
		kafka-console-producer --broker-list proxy:9090 --topic monolog

proxy-consumer:
	docker run -i -t --network=kafkaproxy_default \
		confluentinc/cp-kafka:5.0.0-2 \
		kafka-console-consumer --bootstrap-server proxy:9090 --topic monolog