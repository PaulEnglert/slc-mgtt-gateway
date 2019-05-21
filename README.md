Proxy SLC to MQTT Broker
========================


This forwards tag-base SLC connections to a MQTT Broker.

## Usage


python2.7

    python gateway.py -h


docker

    docker run -it paulenglert/slc-mqtt-gateway:latest -h


Example:

     python gateway.py \
        --log-level trace \
        -p '124.124.124.1/N54:60' \
        -p '124.124.124.1/N52:3' \
        --slc-poll-interval 0.2 \
        --mqtt-topic-prefix '/idatase'


Data is published to the MQTT broker as JSON data with the following format:

    {
        'value': <any>,
        'timestamp': <iso format>
    }


The topics are constructed based on the mqtt prefix, the PLC address and the tag names:

    /myprefix/xxx.xxx.xxx.xxx/NXX:Y
    <mqtt-topic-prefix>/ip-address/tag-name
    /idatase/142.311.211.4/N54:1
