sudo docker run -d -p 9200:9200 -p 9300:9300 --name elasticnode -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.12.1
sudo docker run --link elasticnode:elasticsearch -d -p 5601:5601 --name kibananode docker.elastic.co/kibana/kibana:7.12.1
sudo docker run --network host --name bitcoin2kafka -d massudavide/bitcoin2kafka:latest
sudo docker run -d -p 8081:8081 -p 27017:27017 --name mongodb mongo
sudo docker run --network host --name kafka2mongodb massudavide/kafka2mongodb:latest
