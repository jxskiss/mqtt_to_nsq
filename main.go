/*
 Author: jxskiss <jxskiss@126.com>
 Created: 2017-07-08 00:54
*/

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/timer_metrics"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/nsqio/go-nsq"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

const (
	NsqModeRoundRobin = iota
	NsqModeHostPool
)

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}

type ToNsqMessage struct {
	Topic     string `json:"topic"`
	UUID      string `json:"uuid,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Payload   string `json:"payload"`
}

func (m *ToNsqMessage) TouchUUID() error {
	if u, e := uuid.NewRandom(); e != nil {
		return e
	} else {
		m.UUID = u.String()
		return nil
	}
}

func (m *ToNsqMessage) TouchTimestamp() {
	now := time.Now()
	m.Timestamp = fmt.Sprintf("%d%09d", now.Unix(), now.Nanosecond())
}

type MessageHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platform
	counter uint64

	addresses StringArray
	producers map[string]*nsq.Producer
	mode      int
	hostPool  hostpool.HostPool
	respChan  chan *nsq.ProducerTransaction

	perAddressMetrics map[string]*timer_metrics.TimerMetrics
	timerMetrics      *timer_metrics.TimerMetrics
}

func NewMessageHandler(addresses StringArray, mode string) *MessageHandler {
	ph := &MessageHandler{
		addresses: addresses,
		producers: make(map[string]*nsq.Producer),
		hostPool:  hostpool.New(addresses),
		respChan:  make(chan *nsq.ProducerTransaction, len(addresses)),

		perAddressMetrics: make(map[string]*timer_metrics.TimerMetrics),
		timerMetrics:      timer_metrics.NewTimerMetrics(*m2nCfg.nsqStatusEvery, "[aggregate]:"),
	}

	switch mode {
	case "round-robin":
		ph.mode = NsqModeRoundRobin
	case "hostpool", "epsilon-greedy":
		ph.mode = NsqModeHostPool
	}

	for _, addr := range ph.addresses {
		// have already connected to the address
		if _, ok := ph.producers[addr]; ok {
			continue
		}
		producer, err := nsq.NewProducer(addr, nsqCfg)
		if err != nil {
			log.Fatalf("Failed to create nsq.Producer: %s\n", err)
		}
		if err = producer.Ping(); err != nil {
			log.Fatalf("Failed to ping nsqd: %s, maybe misconfigured", err)
		}
		ph.producers[addr] = producer

		ph.perAddressMetrics[addr] = timer_metrics.NewTimerMetrics(*m2nCfg.nsqStatusEvery, fmt.Sprintf("[%s]:", addr))
	}

	if len(ph.addresses) == 1 {
		// disable per address metrics since there is only one address
		ph.perAddressMetrics[ph.addresses[0]] = timer_metrics.NewTimerMetrics(0, "")
	}

	return ph
}

func (ph *MessageHandler) responder() {
	var startTime time.Time
	var address string
	var hostPoolResponse hostpool.HostPoolResponse

	for t := range ph.respChan {
		switch ph.mode {
		case NsqModeRoundRobin:
			startTime = t.Args[0].(time.Time)
			hostPoolResponse = nil
			address = t.Args[1].(string)
		case NsqModeHostPool:
			startTime = t.Args[0].(time.Time)
			hostPoolResponse = t.Args[1].(hostpool.HostPoolResponse)
			address = hostPoolResponse.Host()
		}

		success := t.Error == nil
		if hostPoolResponse != nil {
			if !success {
				hostPoolResponse.Mark(errors.New("failed"))
				log.Printf("Failed to publish message: %s\n", t.Error)
				// TODO: do we need to republish the message?
			} else {
				hostPoolResponse.Mark(nil)
			}
		}

		ph.perAddressMetrics[address].Status(startTime)
		ph.timerMetrics.Status(startTime)
	}
}

func (ph *MessageHandler) prepareMessage(mqttMsg MQTT.Message) ([]byte, error) {
	var payload = mqttMsg.Payload()
	var err error
	if *m2nCfg.srcMsgEncoding == "gbk" {
		reader := transform.NewReader(bytes.NewReader(payload), simplifiedchinese.GBK.NewDecoder())
		payload, err = ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}
	}

	if m2nCfg.msgTrimEnabled {
		prefix := m2nCfg.msgTrimPrefix
		postfix := m2nCfg.msgTrimPostfix
		payload = postfix.ReplaceAll(
			prefix.ReplaceAll(payload, []byte("")), []byte(""))
		if len(payload) == 0 {
			return nil, errors.New("empty")
		}
	}

	nsqMsg := ToNsqMessage{
		Topic:   mqttMsg.Topic(),
		Payload: string(payload),
	}
	if *m2nCfg.msgWithUUID {
		nsqMsg.TouchUUID()
	}
	if *m2nCfg.msgWithTimestamp {
		nsqMsg.TouchTimestamp()
	}

	return json.Marshal(nsqMsg)
}

func (ph *MessageHandler) HandleMessage(client MQTT.Client, msg MQTT.Message) {
	nsqMsg, err := ph.prepareMessage(msg)
	if err != nil {
		if err.Error() != "empty" {
			log.Printf("Topic: %s, error preparing msg: %s\n", msg.Topic(), err)
		}
		return
	}

	startTime := time.Now()
	switch ph.mode {
	case NsqModeRoundRobin:
		counter := atomic.AddUint64(&ph.counter, 1)
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		p := ph.producers[addr]
		err = p.PublishAsync(*m2nCfg.nsqTopic, nsqMsg, ph.respChan, startTime, addr)
	case NsqModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		p := ph.producers[hostPoolResponse.Host()]
		err := p.PublishAsync(*m2nCfg.nsqTopic, nsqMsg, ph.respChan, startTime, hostPoolResponse)
		if err != nil {
			hostPoolResponse.Mark(err)
		}
	}
}

type m2nConfig struct {
	mqttTopics     StringArray
	mqttServer     *string
	mqttUsername   *string
	mqttPassword   *string
	mqttClientId   *string
	mqttQoS        *int
	srcMsgEncoding *string

	nsqdTCPAddrs   StringArray
	nsqTopic       *string
	nsqMode        *string
	nsqStatusEvery *int

	msgWithUUID      *bool
	msgWithTimestamp *bool
	msgTrimRegex     *string
	msgTrimEnabled   bool
	msgTrimPrefix    *regexp.Regexp
	msgTrimPostfix   *regexp.Regexp
}

var (
	hostname, _ = os.Hostname()
	mqttOpts    MQTT.ClientOptions
	nsqCfg      = nsq.NewConfig()

	m2nCfg = m2nConfig{
		mqttServer:     flag.String("mqtt-server", "tcp://127.0.0.1:1883", `The full url of the MQTT server to connect to, ex: "tcp://127.0.0.1:1883"`),
		mqttUsername:   flag.String("mqtt-username", "", "Username to authenticate to the MQTT server"),
		mqttPassword:   flag.String("mqtt-password", "", "Password to match username"),
		mqttClientId:   flag.String("mqtt-client-id", hostname+strconv.Itoa(time.Now().Second()), "A client id for the MQTT connection"),
		mqttQoS:        flag.Int("mqtt-qos", 0, "The QoS to subscribe to MQTT messages at"),
		srcMsgEncoding: flag.String("src-msg-encoding", "utf8", "The source message encoding, utf8 or gbk, the message will be send to nsq in utf8 encoding"),

		nsqTopic:       flag.String("nsq-topic", "mqtt-to-nsq-messages", "The destinatioon NSQ topic name"),
		nsqMode:        flag.String("nsq-mode", "hostpool", "the upstream request mode, otions: round-robin, hostpool (default), epsilon-greedy"),
		nsqStatusEvery: flag.Int("status-every", 250, "The # of requests between logging status, 0 disables"),

		msgWithUUID:      flag.Bool("with-uuid", false, "Add random uuid to messages"),
		msgWithTimestamp: flag.Bool("with-timestamp", false, "Add timestamp to messages"),
		// `[\x{0000}\x{0001}\x{0004}\x{001b}\x{001d}\x{001e}\x{0085}\x{00a0}\t\n\v\f\r ]+`
		msgTrimRegex:   flag.String("trim-regex", "", `Trim messages before sending to nsq, empty messages will be dropped, ex: "[\x{0000}\t\n\v\f\r ]+"`),
		msgTrimEnabled: false,
	}
)

func init() {
	flag.Var(&m2nCfg.mqttTopics, "mqtt-topic", `The MQTT topic to subscribe to (may be given multiple times, default "#")`)
	flag.Var(&m2nCfg.nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times, send to all)")

	flag.Var(&nsq.ConfigFlag{nsqCfg}, "nsq-producer-opt", "Option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqCfg.UserAgent = fmt.Sprintf("mqtt_to_nsq paho.mqtt/1.0.0 go-nsq/%s", nsq.VERSION)

	flag.Parse()

	if len(*m2nCfg.nsqTopic) == 0 {
		log.Fatal("--nsq-topic required")
	}
	if len(m2nCfg.nsqdTCPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address required")
	}

	mqttOpts = MQTT.ClientOptions{
		ClientID:             *m2nCfg.mqttClientId,
		CleanSession:         true,
		Username:             *m2nCfg.mqttUsername,
		Password:             *m2nCfg.mqttPassword,
		MaxReconnectInterval: 1 * time.Second,
		KeepAlive:            30 * time.Second,
		TLSConfig:            tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert},
	}
	mqttOpts.AddBroker(*m2nCfg.mqttServer)

	if *m2nCfg.srcMsgEncoding != "utf8" && *m2nCfg.srcMsgEncoding != "gbk" {
		log.Fatalf("Unsupported message encoding: %s\n", *m2nCfg.srcMsgEncoding)
	}

	if *m2nCfg.msgTrimRegex != "" {
		log.Printf("Trim messages using regex: %s", *m2nCfg.msgTrimRegex)
		m2nCfg.msgTrimEnabled = true
		m2nCfg.msgTrimPrefix = regexp.MustCompile("^" + *m2nCfg.msgTrimRegex)
		m2nCfg.msgTrimPostfix = regexp.MustCompile(*m2nCfg.msgTrimRegex + "$")
	}
}

func main() {
	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	handler := NewMessageHandler(m2nCfg.nsqdTCPAddrs, *m2nCfg.nsqMode)
	for i := 0; i < len(m2nCfg.nsqdTCPAddrs); i++ {
		go handler.responder()
	}

	mqttOpts.OnConnect = func(c MQTT.Client) {
		if len(m2nCfg.mqttTopics) == 0 {
			m2nCfg.mqttTopics = append(m2nCfg.mqttTopics, "#")
		}
		qos := *m2nCfg.mqttQoS
		for _, t := range m2nCfg.mqttTopics {
			if token := c.Subscribe(t, byte(qos), handler.HandleMessage); token.Wait() && token.Error() != nil {
				panic(token.Error())
			} else {
				log.Printf("Subscribed to topic %s with QoS %d\n", t, qos)
			}
		}
	}
	client := MQTT.NewClient(&mqttOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		log.Printf("Connected to %s\n", *m2nCfg.mqttServer)
	}

	select {
	case <-termChan:
		fmt.Println("Terminal signal received, exiting")
	case <-stopChan:
	}

	for _, producer := range handler.producers {
		producer.Stop()
	}
}
