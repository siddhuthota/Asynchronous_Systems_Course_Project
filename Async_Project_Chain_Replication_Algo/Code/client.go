package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

type Reply struct {
	ReqID      string
	AccountNum string
	Outcome    int
	Balance    float64
}

type Query struct {
	ReqID      string
	AccountNum string
}

type Update struct {
	UpdateType int // 0 - Deposit, 1 - Withdraw, 2 - Transfer
	ReqID      string
	AccountNum string
	Amount     float64
	DestAccountNum string
	DestBank string
}

type ClientRequest struct {
	ReqID             string
	RequestType       int
	RequestUpdateType int
	AccountNum        string
	DestAccountNum    string
	Amount            float64
	Bank              string
	DestBank					string
	IsProcessed       bool
	Timeout           int
	Probability       int
	Retries           int
	IsLeftOut					bool
	TimeoutAttempts   int
}

type ClientConfig struct {
	ClientId          string
	HeadServerAddress string
	TailServerAddress string
	MyUDPServer       string
	MyUdpQueryServer  string
	MyUdpUpdateServer string
	MyUDPMasterServer string
	Bank              string
}

type NewConfig struct {
	Role       string
	UdpAddress string
	Bank       string
}

type ReqStatus struct {
	ReqID string
	IsProcessed bool
	From string
	Seq int
}

const (
	Processed               = 0
	InconsistentWithHistory = -1
	InsufficientFunds       = 1
)

const ClientName = "Client_1"

var ClientId string
var Bank string
var HeadServerUDP string
var TailServerUDP string
var MyUDPQuery string
var MyUDPUpdate string
var MyUDPServer string
var MyUDPMasterServer string

// To store Probablities
var probQuery int
var probDeposit int
var probWithdraw int

var dRequests map[string][]ClientRequest
var pRequests map[string][]ClientRequest
var probRequests map[string][]ClientRequest
var configurations map[string][]ClientConfig

var pendingRequestsQ Queue

const DETERMINISTIC_TAG = "DETERMINISTIC"
const CLIENTSERVERS_TAG = "CLIENT_SERVERS"
const PROBABILISTIC_TAG = "PROBABILITIES"

var TAG string
var ExecReqType int // 0 Deterministic and 1 Probablistic
var nProbReq int

var LogStatement *log.Logger

const MAX_RETRIES = 3

var Seq int

// var channelIsProcessed chan bool

func main() {
	channelIsProcessed := make(chan ReqStatus)
	probRequests = make(map[string][]ClientRequest, 0)
	probRequests["PROBABILITIES"] = []ClientRequest{}
	loadConfigurations()
	setLogger()

	go listenUDPMasterServer()
	go listenUDP(channelIsProcessed)

	time.Sleep(3*time.Second)

	if TAG == DETERMINISTIC_TAG {
		loadDeterministicRequests()
		ExecReqType = 0
		pushRequestsToQ(ExecReqType)
	} else if TAG == PROBABILISTIC_TAG {
		loadProbabilisticRequests()
		ExecReqType = 1
		pushRequestsToQ(ExecReqType)
	}

	processRequestsInOrder(channelIsProcessed)

	ch := make(chan int)
	v := <-ch
	fmt.Printf("V = \n", v)
}

// Starts TCP server to listen to Master
func listenUDPMasterServer() {
	LogStatement.Printf("Client Master UDP Server Started...\n")

	sPort := MyUDPMasterServer // "127.0.0.1:8080"
	sAddr, err := net.ResolveUDPAddr("udp", sPort)
	if err != nil {
		LogStatement.Printf("Resolving UDP client server address failed : %s \n", err)
	}

	sConn, err := net.ListenUDP("udp", sAddr)
	if err != nil {
		LogStatement.Printf("Listening UDP client server thread failed : %s \n", err)
	}
	defer sConn.Close()

	LogStatement.Printf("Client Master UDP listening on %s \n", sConn.LocalAddr().String())

	for {
		// conn, err := sConn.Accept() // this blocks until connection or error
		jsonBytes := make([]byte, 1024)

		n, udpErr := sConn.Read(jsonBytes)
		if udpErr != nil {
			LogStatement.Printf("Client UDP listening server read failed : %s \n", udpErr)
		}
		defer sConn.Close() // Closes the connection at any cost even if any error occurred also

		var newConfig NewConfig
		err := json.Unmarshal(jsonBytes[:n], &newConfig)
		if err != nil {
			LogStatement.Printf("Client %s TCP Server connection Json unmarshall failed : %s \n", ClientId, err)
		}

		if newConfig.Role == "HEAD" && newConfig.Bank == Bank {
			LogStatement.Printf("Received new Head information from master : %+v \n", newConfig)
			HeadServerUDP = newConfig.UdpAddress
			LogStatement.Printf("Stored new Head information  %s \n", newConfig.UdpAddress)
			// sendSampleUpdateReq()
		} else if newConfig.Role == "TAIL" && newConfig.Bank == Bank {
			LogStatement.Printf("Received new Tail information from master : %+v \n", newConfig)
			TailServerUDP = newConfig.UdpAddress
			LogStatement.Printf("Stored new Tail information %s \n", newConfig.UdpAddress)
		}
	}
}

func sendSampleQueryReq() {
	qId := ClientName
	query := Query{qId, "ICICI2014"}
	// channelIsProcessed chan bool
	channelIsProcessed := make(chan ReqStatus)
	sendQueryReqToTail(query, channelIsProcessed)
}

func sendSampleUpdateReq() {
	Id := ClientName
	update := Update{0, Id, "ICICI2014", 20.0, "", ""}
	sendUpdateReqToHead(update)
}

func pushRequestsToQ(execReqType int) {
	pendingReqsQTemp := NewQueue(1)

	if execReqType == 0 {
		max := len(dRequests[DETERMINISTIC_TAG])
		for i := 0; i < max; i++ {
			clientReq := dRequests[DETERMINISTIC_TAG][i]
			pendingReqsQTemp.Push(&clientReq)
		}

		pendingRequestsQ = *pendingReqsQTemp

	} else if execReqType == 1 {
		loadProbabalities()
		loadProbablisticReqs()

		max := len(probRequests[PROBABILISTIC_TAG])
		for i := 0; i < max; i++ {
			clientReq := probRequests[PROBABILISTIC_TAG][i]
			pendingReqsQTemp.Push(&clientReq)
		}

		pendingRequestsQ = *pendingReqsQTemp
	}
}

func loadProbablisticReqs() {
	// pendingReqsQTemp := NewQueue(1)

	firstLimit := probQuery
	secondLimit := probQuery + probDeposit
	allLimit := nProbReq

	for allLimit > 0 {
		random := rand.Intn(10)
		random = random + 1
		if random <= firstLimit && probQuery > 0 {
			clientReq := pRequests[PROBABILISTIC_TAG][0]
			clientReq.ReqID = "CLQ_" + ClientId + "_" + strconv.Itoa(rand.Intn(10))
			clientReq.AccountNum = clientReq.Bank + strconv.Itoa(rand.Intn(10))
			// pendingReqsQTemp.Push(&clientReq)
			probRequests[PROBABILISTIC_TAG] = append(probRequests[PROBABILISTIC_TAG], clientReq)
			allLimit = allLimit - 1
			probQuery = probQuery - 1
		} else if random > firstLimit && random <= secondLimit && probDeposit > 0 {
			clientReq := pRequests[PROBABILISTIC_TAG][1]
			clientReq.ReqID = "CLU_" + ClientId + "_" + strconv.Itoa(rand.Intn(10))
			clientReq.AccountNum = clientReq.Bank + strconv.Itoa(rand.Intn(10))
			clientReq.Amount = /*rand.Intn(100)*/ rand.Float64()
			// pendingReqsQTemp.Push(&clientReq)
			probRequests[PROBABILISTIC_TAG] = append(probRequests[PROBABILISTIC_TAG], clientReq)
			allLimit = allLimit - 1
			probDeposit = probDeposit - 1
		} else if random > secondLimit && random <= nProbReq && probWithdraw > 0 {
			clientReq := pRequests[PROBABILISTIC_TAG][2]
			clientReq.ReqID = "CLU_" + ClientId + "_" + strconv.Itoa(rand.Intn(10))
			clientReq.AccountNum = clientReq.Bank + strconv.Itoa(rand.Intn(10))
			clientReq.Amount = /*rand.Intn(100)*/ rand.Float64()
			// pendingReqsQTemp.Push(&clientReq)
			probRequests[PROBABILISTIC_TAG] = append(probRequests[PROBABILISTIC_TAG], clientReq)
			allLimit = allLimit - 1
			probWithdraw = probWithdraw - 1
		}
	}

	// pendingRequestsQ = *pendingReqsQTemp
	// pRequests[PROBABILISTIC_TAG] = probRequests
	LogStatement.Printf("Random generated pendingRequestsQ :: %+v", probRequests)
}

func loadProbabalities() {
	max := len(pRequests[PROBABILISTIC_TAG])
	for i := 0; i < max; i++ {
		clientReq := pRequests[PROBABILISTIC_TAG][i]
		if clientReq.RequestType == 0 {
			probQuery = clientReq.Probability // * nProbReq
		} else if clientReq.RequestType == 1 {
			if clientReq.RequestUpdateType == 0 {
				probDeposit = clientReq.Probability // * nProbReq
			} else if clientReq.RequestUpdateType == 1 {
				probWithdraw = clientReq.Probability // * nProbReq
			}
		}
	}
}

func loadConfigurations() {
	bankName := os.Args[1]
	file, fileOpenError := os.OpenFile("../Config/Clients/" + bankName + "/Client_Config.json", os.O_RDWR, 0666)
	configurations = make(map[string][]ClientConfig, 0)
	fileOpenError = json.NewDecoder(file).Decode(&configurations)
	if fileOpenError != nil {
		fmt.Printf("Opening %s Client_Config.json failed : %s \n", bankName, fileOpenError)
	}

	clientIndex, e := strconv.Atoi(os.Args[2]) // arg 1 is client id
	if e != nil {
		fmt.Printf("Server index input arguments (ClientId) error %s \n", e)
	}

	configuration := configurations[CLIENTSERVERS_TAG][clientIndex]
	fmt.Printf("My Configuration :: %+v\n", configuration)

	TailServerUDP = configuration.TailServerAddress
	HeadServerUDP = configuration.HeadServerAddress
	MyUDPServer = configuration.MyUDPServer
	MyUDPUpdate = configuration.MyUdpUpdateServer
	MyUDPQuery = configuration.MyUdpQueryServer
	MyUDPMasterServer = configuration.MyUDPMasterServer
	ClientId = configuration.ClientId
	Bank = configuration.Bank

	// To decide, what requests are to be sent to server chain
	// 0 - Iterative ; 1 - Random
	seqType, err := strconv.Atoi(os.Args[3]) // Arg 2 is request type
	if err != nil {
		fmt.Printf("Server index input arguments (Requests Type) error : %s \n", err)
	}

	if seqType == 0 {
		TAG = DETERMINISTIC_TAG
	} else if seqType == 1 {
		TAG = PROBABILISTIC_TAG
	}

	fmt.Printf("Chosen type of requests to server are based on %s \n", TAG)
	nProbReq = 10
	Seq = 0
	/*if (seqType == 1) {
		// Number of Requests
		nProbReq, err := strconv.Atoi(os.Args[3]) // No of prob Req
		if err != nil {
					fmt.Println("Server index input arguments (No of Requests) error ", err)
		}
	}*/
}

func loadDeterministicRequests() {
	fileIndex := os.Args[4] // arg 1 is client id

	fileName := "../Config/Requests/"+ Bank + "/Client_" + ClientId + "_" + fileIndex + "_requests.json"
	file, fileOpenError := os.OpenFile(fileName, os.O_RDWR, 0666)
	dRequests = make(map[string][]ClientRequest, 0)
	fileOpenError = json.NewDecoder(file).Decode(&dRequests)
	if fileOpenError != nil {
		LogStatement.Printf("Opening %s.json failed : %s \n",fileName ,fileOpenError)
	}

	LogStatement.Printf("Deterministic Requests :: %+v \n", dRequests)
}

func loadProbabilisticRequests() {
	fileIndex := os.Args[4] // arg 1 is client id

	fileName := "../Config/Client_" + ClientId + "_" + fileIndex + "_probablities.json"
	file, fileOpenError := os.OpenFile(fileName, os.O_RDWR, 0666)
	if fileOpenError != nil {
		LogStatement.Printf("Opening Clients_probablities.json failed : %s \n", fileOpenError)
	}

	pRequests = make(map[string][]ClientRequest, 0)
	fileOpenError = json.NewDecoder(file).Decode(&pRequests)
	if fileOpenError != nil {
		LogStatement.Printf("Decode failed : %s \n", fileOpenError)
	}

	LogStatement.Printf("Probabilistic Requests :: %+v", pRequests)
}

func processRequestsInOrder(channelIsProcessed chan ReqStatus) {
	LogStatement.Printf("Processing Requests in order...\n")

	for !pendingRequestsQ.IsEmpty() {
		clientReq := pendingRequestsQ.Top()
		LogStatement.Printf("Present Processing Request :: %+v", clientReq)

		if clientReq.RequestType == 0 {
			sendQueryReq(clientReq, channelIsProcessed)
		} else {
			sendUpdateReq(clientReq, channelIsProcessed)
		}

		pendingRequestsQ.Pop()
	}
}

func sendQueryReq(req *ClientRequest, channelIsProcessed chan ReqStatus) {
	query := Query{req.ReqID, req.AccountNum}
	req.Retries++;
	go sendQueryReqToTail(query, channelIsProcessed)
	go TimerTick(req, channelIsProcessed)
	reqStatus := <- channelIsProcessed
	req.IsProcessed = reqStatus.IsProcessed;
	isProcessed := reqStatus.IsProcessed

	if TAG == DETERMINISTIC_TAG {
		for i := 0; i < len(dRequests[DETERMINISTIC_TAG]); i++ {
			if dRequests[DETERMINISTIC_TAG][i].ReqID == req.ReqID {
				dRequests[DETERMINISTIC_TAG][i].IsProcessed = isProcessed
				if dRequests[DETERMINISTIC_TAG][i].IsLeftOut == false && dRequests[DETERMINISTIC_TAG][i].IsProcessed == false {
					if (req.Retries == dRequests[DETERMINISTIC_TAG][i].TimeoutAttempts) {
						dRequests[DETERMINISTIC_TAG][i].Retries = req.Retries;
						dRequests[DETERMINISTIC_TAG][i].IsLeftOut = true;
						LogStatement.Printf("Leftout Request after maximum number of tries : %+v \n", req.ReqID )
						LogStatement.Println("*******************************************************************************************************")
					} else {
						dRequests[DETERMINISTIC_TAG][i].Retries = req.Retries;
						sendQueryReq(&dRequests[DETERMINISTIC_TAG][i], channelIsProcessed)
					}
				}
			}
		}
	} else {
		for i := 0; i < len(probRequests[PROBABILISTIC_TAG]); i++ {
			if probRequests[PROBABILISTIC_TAG][i].ReqID == req.ReqID {
				probRequests[PROBABILISTIC_TAG][i].IsProcessed = isProcessed
				if probRequests[PROBABILISTIC_TAG][i].IsLeftOut == false && probRequests[PROBABILISTIC_TAG][i].IsProcessed == false {
					if (req.Retries == probRequests[PROBABILISTIC_TAG][i].TimeoutAttempts) {
						probRequests[PROBABILISTIC_TAG][i].Retries = req.Retries;
						probRequests[PROBABILISTIC_TAG][i].IsLeftOut = true;
						LogStatement.Printf("Leftout Request after maximum number of tries : %+v \n", req.ReqID )
						LogStatement.Println("*******************************************************************************************************")
					} else {
						probRequests[PROBABILISTIC_TAG][i].Retries = req.Retries;
						sendQueryReq(&probRequests[PROBABILISTIC_TAG][i], channelIsProcessed)
					}
				}
			}
		}
	}
}

func sendUpdateReq(req *ClientRequest, channelIsProcessed chan ReqStatus) {
	update := Update{req.RequestUpdateType, req.ReqID, req.AccountNum, req.Amount, req.DestAccountNum, req.DestBank}
	req.Retries++;
	sendUpdateReqToHead(update)
	go TimerTick(req, channelIsProcessed)
	reqStatus := <- channelIsProcessed
	req.IsProcessed = reqStatus.IsProcessed;
	isProcessed := reqStatus.IsProcessed
	// fmt.Println("Recieved isProcessed from channelIsProcessed :: ", reqStatus)

	if TAG == DETERMINISTIC_TAG {
		for i := 0; i < len(dRequests[DETERMINISTIC_TAG]); i++ {
			if dRequests[DETERMINISTIC_TAG][i].ReqID == req.ReqID {
				dRequests[DETERMINISTIC_TAG][i].IsProcessed = isProcessed
				if dRequests[DETERMINISTIC_TAG][i].IsLeftOut == false && dRequests[DETERMINISTIC_TAG][i].IsProcessed == false {
					if (req.Retries == dRequests[DETERMINISTIC_TAG][i].TimeoutAttempts) {
						dRequests[DETERMINISTIC_TAG][i].Retries = req.Retries;
						dRequests[DETERMINISTIC_TAG][i].IsLeftOut = true;
						LogStatement.Printf("Leftout Request after maximum number of tries : %+v \n", req.ReqID )
						LogStatement.Println("*******************************************************************************************************")
					} else {
						dRequests[DETERMINISTIC_TAG][i].Retries = req.Retries;
						sendUpdateReq(&dRequests[DETERMINISTIC_TAG][i], channelIsProcessed)
					}
				}
			}
		}
	} else {
		for i := 0; i < len(probRequests[PROBABILISTIC_TAG]); i++ {
			if probRequests[PROBABILISTIC_TAG][i].ReqID == req.ReqID {
				probRequests[PROBABILISTIC_TAG][i].IsProcessed = isProcessed
				if probRequests[PROBABILISTIC_TAG][i].IsLeftOut == false && probRequests[PROBABILISTIC_TAG][i].IsProcessed == false {
					if (req.Retries == probRequests[PROBABILISTIC_TAG][i].TimeoutAttempts) {
						probRequests[PROBABILISTIC_TAG][i].Retries = req.Retries;
						probRequests[PROBABILISTIC_TAG][i].IsLeftOut = true;
						LogStatement.Printf("Leftout Request after maximum number of tries : %+v \n", req.ReqID )
						LogStatement.Println("*******************************************************************************************************")
					} else {
						probRequests[PROBABILISTIC_TAG][i].Retries = req.Retries;
						sendUpdateReq(&probRequests[PROBABILISTIC_TAG][i], channelIsProcessed)
					}
				}
			}
		}
	}
}

func listenUDP(channelIsProcessed chan ReqStatus) {
	LogStatement.Printf("Client UDP Server Started...\n")

	sPort := MyUDPServer // "127.0.0.1:8080"
	sAddr, err := net.ResolveUDPAddr("udp", sPort)
	if err != nil {
		LogStatement.Printf("Resolving UDP client server address failed : %s \n", err)
	}

	sConn, err := net.ListenUDP("udp", sAddr)
	if err != nil {
		LogStatement.Printf("Listening UDP client server thread failed : %s \n", err)
	}
	defer sConn.Close()

	LogStatement.Printf("Client UDP listening on %s \n", sConn.LocalAddr().String())

	for {
		var reply Reply
		jsonBytes := make([]byte, 1024)
		// n, cAddress, udpErr := sConn.ReadFromUDP(jsonBytes)
		n, udpErr := sConn.Read(jsonBytes)
		if udpErr != nil {
			LogStatement.Printf("Client UDP listening server read failed : %s \n", udpErr)
		}

		// LogStatement.Printf("Received from client bytes\n", jsonBytes)
		jsonErr := json.Unmarshal(jsonBytes[:n], &reply)
		if jsonErr != nil {
			LogStatement.Printf("Client UDP listening Json unmarshall failed : %s \n", jsonErr)
		}

		LogStatement.Printf("Update reply received from tail server : %+v \n", reply)

		if TAG == DETERMINISTIC_TAG {
			for i := 0; i < len(dRequests[DETERMINISTIC_TAG]); i++ {
				if dRequests[DETERMINISTIC_TAG][i].ReqID == reply.ReqID {
					dRequests[DETERMINISTIC_TAG][i].IsProcessed = true
					if dRequests[DETERMINISTIC_TAG][i].IsLeftOut != true {
						Seq++
						reqStatus := ReqStatus{reply.ReqID, true, "listenUDP", Seq}
						channelIsProcessed <- reqStatus
					}
				}
			}
		} else if TAG == PROBABILISTIC_TAG{
			for i := 0; i < len(probRequests[PROBABILISTIC_TAG]); i++ {
				if probRequests[PROBABILISTIC_TAG][i].ReqID == reply.ReqID {
					probRequests[PROBABILISTIC_TAG][i].IsProcessed = true
					if probRequests[PROBABILISTIC_TAG][i].IsLeftOut != true {
						Seq++
						reqStatus := ReqStatus{reply.ReqID,true, "listenUDP", Seq}
						channelIsProcessed <- reqStatus
					}
				}
			}
		}

		// To say that process has been processed
		// channelIsProcessed <- true

		LogStatement.Println("*******************************************************************************************************")
	}
}

func sendUpdateReqToHead(update Update) {
	LogStatement.Printf("Contacting Head to send update request... %s \n", HeadServerUDP)

	sPort := HeadServerUDP
	sAddr, err := net.ResolveUDPAddr("udp", sPort)
	if err != nil {
		LogStatement.Printf("Resolving Head UDP server address failed : %s", err)
	}

	cPort := MyUDPUpdate
	cAddr, err := net.ResolveUDPAddr("udp", cPort)
	if err != nil {
		LogStatement.Printf("Resolving Client UDP client address failed : %s", err)
	}

	cConn, err := net.DialUDP("udp", cAddr, sAddr)
	if err != nil {
		LogStatement.Printf("Dailing Head UDP thread failed : %s \n", err)
	}
	defer cConn.Close()

	LogStatement.Printf("Head UDP Connection established \n")

	message, err := json.Marshal(update)

	if err != nil {
		LogStatement.Printf("Update request Json marshall failed : %s \n", err)
	}

	LogStatement.Println("UDP Sent message :: ", string(message))
	_, wErr := cConn.Write([]byte(message))
	if wErr != nil {
		LogStatement.Printf("Update request write to Head failed : %s \n", err)
	}

	LogStatement.Printf("Update request sent to Head successfully \n")
}

func sendQueryReqToTail(query Query, channelIsProcessed chan ReqStatus) {
	LogStatement.Printf("Contacting Tail to send query request...%s \n", TailServerUDP)

	sPort := TailServerUDP
	sAddr, err := net.ResolveUDPAddr("udp", sPort)
	if err != nil {
		LogStatement.Printf("Resolving Tail UDP server address failed: %s \n", err)
		return
	}

	cPort := MyUDPQuery
	cAddr, err := net.ResolveUDPAddr("udp", cPort)
	if err != nil {
		LogStatement.Printf("Resolving Client UDP client address failed: %s \n", err)
		return
	}

	cConn, err := net.DialUDP("udp", cAddr, sAddr)
	if err != nil {
		LogStatement.Printf("Dailing Tail UDP thread failed : %s \n", err)
		return
	}
	defer cConn.Close()

	LogStatement.Printf("Tail UDP Connection established \n")

	// query := Query{ClientName, "ICICI2014"}
	message, err := json.Marshal(query)

	if err != nil {
		LogStatement.Printf("Query request Json marshall failed : %s \n", err)
		return
	}

	_, wErr := cConn.Write([]byte(message))
	if wErr != nil {
		LogStatement.Printf("Query request write to Head failed : %s \n", err)
		return
	}

	LogStatement.Printf("Query request sent to Tail successfully \n")

	var reply Reply
	jsonReplyBytes := make([]byte, 1024)
	n, sAddress, udpErr := cConn.ReadFromUDP(jsonReplyBytes)
	if udpErr != nil {
		LogStatement.Printf("Query reply read failed : %s %s and reply address %s \n", udpErr, sAddr, sAddress)
		return
	}

	// LogStatement.Printf("Received from client bytes\n", jsonBytes)
	jsonErr := json.Unmarshal(jsonReplyBytes[:n], &reply)
	if jsonErr != nil {
		LogStatement.Printf("Query reply Json unmarshall failed : %s \n", jsonErr)
		return
	}

	cConn.Close()

	// To say that process has been processed
	if (!(reply.ReqID == "")) {
		Seq++
		reqStatus := ReqStatus{reply.ReqID,true, "listenUDPQuery",Seq}
		channelIsProcessed <- reqStatus
		// channelIsProcessed <- true
		LogStatement.Printf("Query reply received from tail : %+v \n", reply)
		LogStatement.Println("Balance in account is : ", reply.Balance)
		LogStatement.Println("********************************************************************************************************")
	} else {
		Seq++
		reqStatus := ReqStatus{reply.ReqID,false, "listenUDPQuery",Seq}
		channelIsProcessed <- reqStatus
		// channelIsProcessed <- false
	}
}

//----------------------- Helper Functions----------------------------//
// NewQueue returns a new queue with the given initial size.
func NewQueue(size int) *Queue {
	return &Queue{
		nodes: make([]*ClientRequest, size),
		size:  size,
	}
}

// Queue is a basic FIFO queue based on a circular list that resizes as needed.
type Queue struct {
	nodes []*ClientRequest
	size  int
	head  int
	tail  int
	count int
}

func (q *Queue) IsEmpty() bool {
	isEmpty := false

	if q.head == q.tail && q.count == 0 {
		isEmpty = true
	}

	return isEmpty
}

// Push adds a node to the queue.
func (q *Queue) Push(n *ClientRequest) {
	if q.head == q.tail && q.count > 0 {
		nodes := make([]*ClientRequest, len(q.nodes)+q.size)
		copy(nodes, q.nodes[q.head:])
		copy(nodes[len(q.nodes)-q.head:], q.nodes[:q.head])
		q.head = 0
		q.tail = len(q.nodes)
		q.nodes = nodes
	}
	q.nodes[q.tail] = n
	q.tail = (q.tail + 1) % len(q.nodes)
	q.count++
}

// Pop removes and returns a node from the queue in first to last order.
func (q *Queue) Pop() *ClientRequest {
	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	q.head = (q.head + 1) % len(q.nodes)
	q.count--
	return node
}

// Top returns a node from the queue which is in top
func (q *Queue) Top() *ClientRequest {
	if q.count == 0 {
		return nil
	}
	node := q.nodes[q.head]
	return node
}

func setLogger() {
	fileName := "Client_" + ClientId + "_log.log"
	file, err := os.OpenFile("../logs/"+fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Failed to open log file %s \n", err)
	}

	multi := io.MultiWriter(file, os.Stdout)

	prefix := "CLIENT " + ClientId + ": "
	LogStatement = log.New(multi,
		prefix,
		log.Ldate|log.Ltime) // |log.Lshortfile) To include line numbers of logged line
	LogStatement.Printf("Logger set successfully...\n")
}

// Triggers to check servers at regular intervals
func TimerTick(req *ClientRequest, channelIsProcessed chan ReqStatus) {
	LogStatement.Printf("Try %d started for Request : %s \n", req.Retries, req.ReqID)
	ticker := time.NewTicker(time.Duration(req.Timeout) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				if req.IsProcessed == false {
					Seq++
					reqStatus := ReqStatus{req.ReqID, false, "Timer", Seq}
					channelIsProcessed <- reqStatus
					close(quit)
				} else {
					close(quit)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
