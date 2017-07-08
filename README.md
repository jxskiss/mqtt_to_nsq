# mqtt_to_nsq

Usage:

```text
  -mqtt-client-id string
    	A client id for the MQTT connection (default "hostname")
  -mqtt-password string
    	Password to match username
  -mqtt-qos int
    	The QoS to subscribe to MQTT messages at
  -mqtt-server string
    	The full url of the MQTT server to connect to, ex: "tcp://127.0.0.1:1883" (default "tcp://127.0.0.1:1883")
  -mqtt-topic value
    	The MQTT topic to subscribe to (may be given multiple times, default "#")
  -mqtt-username string
    	Username to authenticate to the MQTT server
  -nsq-producer-opt value
    	Option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)
  -nsq-topic string
    	The destinatioon NSQ topic name (default "mqtt-to-nsq-messages")
  -nsqd-tcp-address value
    	nsqd TCP address (may be given multiple times, send to all)
  -src-msg-encoding string
    	The source message encoding, utf8 or gbk, the message will be send to nsq in utf8 encoding (default "utf8")
  -status-every int
    	The # of requests between logging status, 0 disables (default 250)
  -trim-regex string
    	Trim messages before sending to nsq, empty messages will be dropped, ex: "[\x{0000}\t\n\v\f\r ]+"
  -with-timestamp
    	Add timestamp to messages
  -with-uuid
    	Add random uuid to messages
```
