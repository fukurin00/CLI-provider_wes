package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	sxmqtt "github.com/synerex/proto_mqtt"
	proto_wes "github.com/synerex/proto_wes"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	mqttMsgFile     = flag.String("mqttMsgFile", "", "mqtt message file")
	mqttMsg         = flag.String("mqttMsg", "", "mqtt message")
	mqttTopic       = flag.String("mqttTpc", "", "mqtt topic")
	goAmr           = flag.String("goAmr", "", "goAmr <id>,<x>,<y>,<angle>")
	goHuman         = flag.String("goHuman", "", "goHuman <id>,<x>,<y>,<angle>")
	speed           = flag.Float64("speed", 0.0, "set speed")
	setState        = flag.String("setState", "", "setState <mqttMsgFile>")
	wmsCsv          = flag.String("wmsCsv", "", "wms csv file")
	async           = flag.Bool("async", false, "asyncronous for wms")
	sync            = flag.Bool("sync", false, "syncronous simlation for wms")
	onlyHuman       = flag.Bool("onlyHuman", false, "onlyHuman simlation for wms")
	start           = flag.Bool("start", false, "start simulation")
	humanMsg        = flag.String("humanMsg", "", "humanMsg <ID>, <x>, <y>, <shelfID>")
	amrMsg          = flag.String("amrMsg", "", "amrMsg <ID>, <x>, <y>")
	wmsNum          = flag.Int("wmsNum", 0, "wmsCsv's sending item num(default all)")
	sxServerAddress string
)

func sendGoAmr(client *sxutil.SXServiceClient, input string) {
	param := strings.Split(input, ",")

	if len(param) < 4 {
		log.Fatal("Error: please input ID, x, y, angle")
	}

	id := param[0]
	x := param[1]
	y := param[2]
	angle := param[3]

	tpc := fmt.Sprintf("cmd/robot/%s/go", id)

	msg := fmt.Sprintf(`{
		"x":     %s,
		"y":     %s,
		"angle": %s
		}`,
		x, y, angle,
	)

	rcdMqtt := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Time:   ptypes.TimestampNow(),
		Record: []byte(msg),
	}

	out, _ := proto.Marshal(&rcdMqtt)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Print("Connection failure", nerr)
	} else {
		log.Printf("send command go amr")
	}
}

func sendGoHuman(client *sxutil.SXServiceClient, input string) {
	param := strings.Split(input, ",")

	if len(param) < 4 {
		log.Fatal("Error please input ID, x, y, angle")
	}
	id := param[0]
	x := param[1]
	y := param[2]
	angle := param[3]

	tpc := fmt.Sprintf("cmd/human/%s/go", id)

	msg := fmt.Sprintf(`{
		"x":     %s,
		"y":     %s,
		"angle": %s
		}`,
		x, y, angle,
	)

	rcdMqtt := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Time:   ptypes.TimestampNow(),
		Record: []byte(msg),
	}

	out, _ := proto.Marshal(&rcdMqtt)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Print("Connection failure", nerr)
	} else {
		log.Printf("send command go")
	}
}

func sendMqttPublish(client *sxutil.SXServiceClient, tpc string, msg string) {
	rcd := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Record: []byte(msg),
	}

	out, _ := proto.Marshal(&rcd)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	} else {
		log.Printf("send command %s:%s", tpc, msg)
	}
}

func sendMqttMsgFile(client *sxutil.SXServiceClient, tpc string, nfile string) {
	msg, err := ioutil.ReadFile(nfile)
	if err != nil {
		log.Fatal("file loading failer", err)
	}

	rcd := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Record: msg,
	}

	out, _ := proto.Marshal(&rcd)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	} else {
		log.Printf("send command %s:%s", tpc, msg)
	}
}

func sendSpeed(client *sxutil.SXServiceClient, speed float64) {
	strSpeed := strconv.FormatFloat(speed, 'f', 2, 64)

	tpc := "cmd/simulator/set_speed"
	msg := fmt.Sprintf(`{
		"speed": %s
		}`,
		strSpeed,
	)

	rcdMqtt := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Time:   ptypes.TimestampNow(),
		Record: []byte(msg),
	}

	out, _ := proto.Marshal(&rcdMqtt)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	} else {
		log.Printf("send command speed")
	}
}

func sendSetState(client *sxutil.SXServiceClient, nfile string) {
	msg, err := ioutil.ReadFile(nfile)
	if err != nil {
		log.Fatal("file loading failer", err)
	}

	tpc := "cmd/simulator/set_state"
	rcd := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Record: msg,
	}

	out, _ := proto.Marshal(&rcd)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Print("Connection failure", nerr)
	} else {
		log.Printf("send command %s:%s", tpc, msg)
	}
}

func loadCsv(fname string) [][]string {
	f, err := os.Open(fname)
	if err != nil {
		log.Fatal("file loading failer: ", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	content, err := reader.ReadAll()
	if err != nil {
		log.Print(err)
	}
	//スペースを取り除く
	for _, row := range content {
		for _, s := range row {
			s = strings.Replace(s, " ", "", -1)
		}
	}
	return content
}

// wms_orderを送信する、1バッチずつ送信する
func sendWmsCsv(client *sxutil.SXServiceClient, wmsFile string) {
	itemsMap := make(map[int][]*proto_wes.Item)

	content := loadCsv(wmsFile)
	var idList []int
	idHumanMap := make(map[int]int)

	wmsIDIndex := 0
	shelfIDIndex := 2
	humanIDIndex := -1
	//isFullIndex := 4

	for i, row := range content {
		if i == 0 {
			for j, head := range row {
				switch strings.ToLower(head) {
				case "batid", "bat_id", "id", "wmsid", "wms_id":
					wmsIDIndex = j
				case "shelfid", "shelf_id", "location":
					shelfIDIndex = j
				case "isfull":
					//isFullIndex = j
				case "userid", "human":
					humanIDIndex = j
				}
			}
			continue
		}

		wmsID, err := strconv.Atoi(row[wmsIDIndex])

		if err != nil {
			log.Print(err)
		}

		if humanIDIndex != -1 {
			humanID, herr := strconv.Atoi(row[humanIDIndex])
			log.Printf("wms%d send to specific human%d", wmsID, humanID)
			if herr != nil {
				log.Print(herr)
			} else {
				idHumanMap[wmsID] = humanID
			}
		} else {
			idHumanMap[wmsID] = -1
		}

		shelfID := row[shelfIDIndex]

		item := proto_wes.Item{
			ShelfID: shelfID,
		}

		if _, ok := itemsMap[wmsID]; !ok {
			idList = append(idList, wmsID)
		}
		itemsMap[wmsID] = append(itemsMap[wmsID], &item)

		if *wmsNum != 0 {
			if i >= *wmsNum {
				break
			}
		}
	}

	for _, id := range idList {
		rcd := proto_wes.WmsOrder{
			WmsID:   int64(id),
			HumanID: int64(idHumanMap[id]),
			Item:    itemsMap[id],
		}

		out, _ := proto.Marshal(&rcd)
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "CLI_wms_order",
			Cdata: &cont,
		}
		_, nerr := client.NotifySupply(&smo)
		if nerr != nil { // connection failer with current client
			log.Print("Connection failure", nerr)
		}
		//log.Print("send wms_order", rcd)

	}
	log.Print("send wms order")
}

func sendSimlationType(client *sxutil.SXServiceClient, typ string) {
	rcd := proto_wes.SimType{
		SimType: typ,
	}

	out, _ := proto.Marshal(&rcd)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_sim_type",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Print("Connection failure", nerr)
	} else {
		log.Printf("send simType %s", typ)
	}

}

func sendStart(client *sxutil.SXServiceClient) {
	tpc := "cmd/simulator/start"
	msg := ""

	rcdMqtt := sxmqtt.MQTTRecord{
		Topic:  tpc,
		Time:   ptypes.TimestampNow(),
		Record: []byte(msg),
	}

	out, _ := proto.Marshal(&rcdMqtt)
	cont := pb.Content{Entity: out}
	smo := sxutil.SupplyOpts{
		Name:  "CLI_MQTT_Supply",
		Cdata: &cont,
	}

	_, nerr := client.NotifySupply(&smo)
	if nerr != nil { // connection failuer with current client
		log.Printf("Connection failure", nerr)
	} else {
		log.Printf("send command start")
	}
}

//　WESに送るメッセージ定義

type stamp struct {
	Secs  float64 `json:"secs"`
	Nsecs float64 `json:"nsecs"`
}
type header struct {
	Seq      float64 `json:"seq"`
	Stamp    stamp   `json:"stamp"`
	Frame_id string  `json:"frame_id"`
}

type pose struct {
	Position    position    `json:"position"`
	Orientation orientation `json:"orientation"`
}

type position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type orientation struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
	W float64 `json:"w"`
}
type rosPoseMsg struct {
	Header header `json:"header"`
	Pose   pose   `json:"pose"`
}

type humanHeader struct {
	Seq      float64 `json:"seq"`
	Stamp    stamp   `json:"stamp"`
	Frame_id string  `json:"frame_id"`
	Shelf_id string  `json:"shelf_id"`
}

type humanPublishMsg struct {
	Header humanHeader `json:"header"`
	Pose   pose        `json:"pose"`
}

func sendHumanMsg(client *sxutil.SXServiceClient, input string) {
	param := strings.Split(input, ",")

	if len(param) < 4 {
		log.Fatal("Error: please input ID, x, y, shelfID")
	}

	id := param[0]
	x := param[1]
	y := param[2]
	shelfID := param[3]

	fx, err1 := strconv.ParseFloat(x, 64)
	fy, err2 := strconv.ParseFloat(y, 64)
	if err1 != nil || err2 != nil {
		log.Fatal(err1, err2)
	}

	topic := fmt.Sprintf("pos/human/%s/pose", id)

	msg := humanPublishMsg{
		Header: humanHeader{
			Seq: 0,
			Stamp: stamp{
				Secs:  float64(time.Now().Unix()),
				Nsecs: float64(time.Now().UnixNano()),
			},
			Frame_id: id,
			Shelf_id: shelfID,
		},
		Pose: pose{
			Position: position{
				X: fx,
				Y: fy,
				Z: 0.0,
			},
			Orientation: orientation{
				X: 2 * 3.14 * rand.Float64(), //向きは適当に決める
				Y: 2 * 3.14 * rand.Float64(),
				Z: 0.0,
				W: 0.0,
			},
		},
	}

	record, err := json.Marshal(msg)

	if err != nil {
		log.Fatal(err)
	} else {
		rcdMqtt := &sxmqtt.MQTTRecord{
			Topic:  topic,
			Record: record,
		}
		out, _ := proto.Marshal(rcdMqtt)
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "Human_MQTT_Publish",
			Cdata: &cont,
		}
		_, nerr := client.NotifySupply(&smo)
		if nerr != nil { // connection failuer with current client
			log.Print("Connection failure", nerr)
		} else {
			log.Printf("Send Mqtt publish topic %s, message%s", string(topic), string(record))
		}
	}
}

func sendAmrMsg(client *sxutil.SXServiceClient, input string) {
	param := strings.Split(input, ",")

	if len(param) < 3 {
		log.Fatal("Error: please input ID, x, y")
	}

	id := param[0]
	x := param[1]
	y := param[2]

	fx, err1 := strconv.ParseFloat(x, 64)
	fy, err2 := strconv.ParseFloat(y, 64)
	if err1 != nil || err2 != nil {
		log.Fatal(err1, err2)
	}

	topic := fmt.Sprintf("pos/robot/%s/pose", id)

	msg := rosPoseMsg{
		Header: header{
			Seq: 0,
			Stamp: stamp{
				Secs:  float64(time.Now().Unix()),
				Nsecs: float64(time.Now().UnixNano()),
			},
			Frame_id: id,
		},
		Pose: pose{
			Position: position{
				X: fx,
				Y: fy,
				Z: 0.0,
			},
			Orientation: orientation{
				X: 2 * 3.14 * rand.Float64(), //向きは適当に決める
				Y: 2 * 3.14 * rand.Float64(),
				Z: 0.0,
				W: 0.0,
			},
		},
	}

	record, err := json.Marshal(msg)

	if err != nil {
		log.Fatal(err)
	} else {
		rcdMqtt := &sxmqtt.MQTTRecord{
			Topic:  topic,
			Record: record,
		}
		out, _ := proto.Marshal(rcdMqtt)
		cont := pb.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "AMR_MQTT_Publish",
			Cdata: &cont,
		}
		_, nerr := client.NotifySupply(&smo)
		if nerr != nil { // connection failuer with current client
			log.Print("Connection failure", nerr)
		} else {
			log.Printf("Send Mqtt publish topic %s, message%s", string(topic), string(record))
		}
	}
}

func main() {
	log.Printf("Harmoware-WES CLI provider (%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.MQTT_GATEWAY_SVC}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "Harmowware-WES-CLI", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJSON := fmt.Sprintf("{Client:WES-CLI-MQTT}")
	mqttclient := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJSON)

	argJSON2 := fmt.Sprintf("{Client:WES-CLI-WAREHOUSE}")
	wesclient := sxutil.NewSXServiceClient(client, pbase.WAREHOUSE_SVC, argJSON2)

	if *goAmr != "" {
		sendGoAmr(mqttclient, *goAmr)
	}

	if *goHuman != "" {
		sendGoHuman(mqttclient, *goHuman)
	}

	if *mqttMsg != "" {
		sendMqttPublish(mqttclient, *mqttTopic, *mqttMsg)
	}

	if *mqttMsgFile != "" {
		sendMqttMsgFile(mqttclient, *mqttTopic, *mqttMsgFile)
	}

	if *speed != 0.0 {
		sendSpeed(mqttclient, *speed)
	}

	if *setState != "" {
		sendSetState(mqttclient, *setState)
	}

	if *wmsCsv != "" {
		sendWmsCsv(wesclient, *wmsCsv)
	}

	if *async {
		sendSimlationType(wesclient, "asynchronous")
	}

	if *sync {
		sendSimlationType(wesclient, "synchronous")
	}

	if *onlyHuman {
		sendSimlationType(wesclient, "onlyHuman")
	}

	if *start {
		sendStart(mqttclient)
	}

	if *humanMsg != "" {
		sendHumanMsg(mqttclient, *humanMsg)
	}

	if *amrMsg != "" {
		sendAmrMsg(mqttclient, *amrMsg)
	}

	sxutil.CallDeferFunctions() // cleanup!

}
