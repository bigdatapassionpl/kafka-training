
~~~shell
python producer.py -b bootstrap.${GCP_KAFKA}.${GCP_REGION}.managedkafka.${GCP_PROJECT_ID}.cloud.goog:9092

python gcp/producer.py -b bootstrap.mytestkafkacluster.europe-west3.managedkafka.bigdataworkshops.cloud.goog:9092

python google_auth_test.py
~~~
