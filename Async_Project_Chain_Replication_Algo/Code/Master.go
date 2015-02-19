package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

// Ping structure to send ping to master
type Ping struct {
	ServerId string
	Bank     string
}

type ServerConfig struct {
	ServerId         string
	Bank             string
	receivedPing     bool
	TcpAddress       string
	ServerType       string
	UdpQueryAddress  string
	UdpUpdateAddress string
}

type ClientConfig struct {
	ClientId   string
	Bank       string
	UDPMasterAddress string
	UdpAddress string
}

type Message struct {
	From       int
	Type       int
	SubType    int
	Bank       string
	SConfig    ServerConfig
	PingMsg    Ping
	NewRoleMsg NewRole
	NewConfigMsg  NewConfig
	NodeStatus ServerNodeStatus
}

type ClientDetails struct {
	ClientAddress string
}

type NewRole struct {
	Role string
}

type BankName struct {
	Bank string
	TimeOut int
}

type ServerNodeStatus struct {
	ProcessedTransCount int
	AccountsCount       int
	FailedServerIndex   int
}

type NewConfig struct {
	Role       string
	UdpAddress string
	TcpAddress string
	Bank       string
}

// To distiniguish the message from MASTER or SERVER NODE
const (
	MASTER           = 1
	SERVERNODE       = 2
	NEW_NODE         = 3
	NEW_TAIL         = 4
	OLD_TAIL         = 5
	NEW_PREDECESSOR  = 6
	PREDECESSOR      = 7
	NEW_INTERMEDIATE = 8
)

// To distiniguish the message type from MASTER or SERVER NODE
const (
	NODE_JOIN              = 21
	NEW_ROLE               = 22
	CLAIM_ROLE             = 23
	PING                   = 24
	PREDECESSOR_FAILURE    = 25
	PROCESSED_TRANS_STATUS = 26
	SUCCESSOR_FAILURE      = 27
	PROCESSED_TRANS        = 28
	NORMAL_UPDATE_REQ      = 29
	NEW_SUCCESSOR_INFO     = 30
	ACK_TO_REQ 						 = 31
	PROCESS_REQ            = 32
	NEW_NODE_FAILURE       = 33
	TAIL_FAILURE 					 = 34
	HEAD_FAILURE 					 = 35
	TRANSFER_REQ           = 36
	TRANSFER_ACK           = 37
	OTHER_BANK_RELATED     = 38
)

var MyPingTCP string
var MyMiscTCP string
var MyUDP string

var LogStatement *log.Logger

// Map that stores all configurations of client nodes
var serverConfigs map[string][]ServerConfig

// Map that stores all client configurations of all banks
var clientConfigs map[string][]ClientConfig

// Bank names
var banks map[string][]BankName

var newNodeJoinInProgress bool

// Main function
func main() {
	// readConfigValues()
	setLogger()
	getConfigurationsOfBanks()
	newNodeJoinInProgress = false

	for i := 0; i < 2/*len(banks["BANKS"])*/; i++ {
		go TimerTick(banks["BANKS"][i].Bank, banks["BANKS"][i].TimeOut)
	}

	//Bank1 := "ICICI"
	go listenPingTCP()
	listenMiscTCP()
}

func getConfigurationsOfBanks() {
	banksFile, fileOpenError := os.OpenFile("../Config/Master/banks.json", os.O_RDWR, 0666)
	banks = make(map[string][]BankName, 0)
	fileOpenError = json.NewDecoder(banksFile).Decode(&banks)
	if fileOpenError != nil {
	    fmt.Printf("Unable to read Banks.json file : %s \n", fileOpenError)
	}

	LogStatement.Printf("Banks are: %s \n", banks)

	// Getting server configurations from config file
	file, fileOpenError := os.OpenFile("../Config/Master/Master_Servers_Config.json", os.O_RDWR, 0666)
	serverConfigs = make(map[string][]ServerConfig, 0)
	fileOpenError = json.NewDecoder(file).Decode(&serverConfigs)
	if fileOpenError != nil {
		fmt.Printf("Unable to read Master_Servers_Configs.json file : %s \n", fileOpenError)
	}

	LogStatement.Printf("Bank Servers Configurations: %s \n", serverConfigs)

	// Getting Client configurations from config file
	file, fileOpenError = os.OpenFile("../Config/Master/Master_Clients_Config.json", os.O_RDWR, 0666)
	clientConfigs = make(map[string][]ClientConfig, 0)
	fileOpenError = json.NewDecoder(file).Decode(&clientConfigs)
	if fileOpenError != nil {
		fmt.Printf("Unable to read Master_Clients_Config.json file : %s \n", fileOpenError)
	}

	LogStatement.Printf("Bank Clients Configurations: %s \n", serverConfigs)

	MyPingTCP = "localhost:8070"
	MyMiscTCP = "localhost:8071"
	MyUDP = "localhost:8072"
}

// Listens to TCP server to get pings from all Bank servers
func listenPingTCP() {
	LogStatement.Printf("Master Ping TCP Server Started...\n")
	ln, err := net.Listen("tcp", MyPingTCP)

	if err != nil {
		LogStatement.Printf("Master Ping TCP Server listening thread failed : %s \n", err)
	}

	LogStatement.Printf("Master Ping TCP Server Listening... %s \n", MyPingTCP)

	for {
		conn, err := ln.Accept() // this blocks until connection or error

		if err != nil {
			LogStatement.Printf("Master Ping TCP Server listening connection accept error : %s", err)
		}
		defer conn.Close() // Closes the connection at any cost even if any error occurred also

		// After accepting the processes the pings
		go handlePingConn(conn) // a goroutine handles conn so that the loop can accept other connections
	}
}

// Listens to TCP server to get pings from all new Bank servers
func listenMiscTCP() {
	LogStatement.Printf("Master TCP Server Started...\n")
	ln, err := net.Listen("tcp", MyMiscTCP)

	if err != nil {
		LogStatement.Printf("Master TCP Server listening thread failed : %s \n", err)
	}

	LogStatement.Printf("Master TCP Server Listening... %s \n", MyMiscTCP)

	for {
		conn, err := ln.Accept() // this blocks until connection or error

		if err != nil {
			LogStatement.Printf("TCP Server listening connection accept error : %s", err)
		}
		defer conn.Close() // Closes the connection at any cost even if any error occurred also

		// After accepting the processes the pings
		go handleMiscConn(conn) // a goroutine handles conn so that the loop can accept other connections
	}
}

// This function handles pings received by master
func handlePingConn(conn net.Conn) {
	// LogStatement.Printf("Handling predecessor tcp connection accept...\n")
	var msg Message
	jsonBytes := make([]byte, 1024)

	n, jsonErr := conn.Read(jsonBytes)
	if jsonErr != nil {
		LogStatement.Printf("Master TCP server connection Json read failed : %s \n", jsonErr)
	}

	err := json.Unmarshal(jsonBytes[:n], &msg)
	if err != nil {
		LogStatement.Printf("Master TCP server connection Json unmarshall failed : %s \n", err)
	}

	ping := msg.PingMsg
	for i := 0; i < len(serverConfigs[ping.Bank]); i++ {
		if serverConfigs[ping.Bank][i].ServerId == ping.ServerId {
			serverConfigs[ping.Bank][i].receivedPing = true
		}
	}

	LogStatement.Printf("Ping received from server %s belongs to bank %s", ping.ServerId, ping.Bank)
	conn.Close()
}

func handleMiscConn(conn net.Conn) {
	// LogStatement.Printf("Handling predecessor tcp connection accept...\n")
	msg := Message{}
	jsonBytes := make([]byte, 1024)

	n, jsonErr := conn.Read(jsonBytes)
	if jsonErr != nil {
		LogStatement.Printf("Master TCP server connection Json read failed : %s \n", jsonErr)
	}

	err := json.Unmarshal(jsonBytes[:n], &msg)
	if err != nil {
		LogStatement.Printf("Master TCP server connection Json unmarshall failed : %s \n", err)
	}

	if msg.From == NEW_NODE && msg.Type == NODE_JOIN {
		newNodeJoinInProgress = true
		sConfig := msg.SConfig
		length := len(serverConfigs[sConfig.Bank])
		oldTail := serverConfigs[sConfig.Bank][length-1]
		serverConfigs[sConfig.Bank] = append(serverConfigs[sConfig.Bank], sConfig)
		sendTailAbtNewServer(sConfig, oldTail)

	} else if msg.From == NEW_TAIL && msg.Type == CLAIM_ROLE {
		if msg.NewRoleMsg.Role == "TAIL" {
			length := len(serverConfigs[msg.Bank])
			LogStatement.Printf("Changing configuartions in serverConfigs and length of %s server chain is %d\n", msg.Bank, length)
			serverConfigs[msg.Bank][length-2].ServerType = "INTERMEDIATE"
			serverConfigs[msg.Bank][length-1].ServerType = "TAIL"
			newTail := serverConfigs[msg.Bank][length-1]
			LogStatement.Printf("New New Tail is %s \n", newTail)
			newNodeJoinInProgress = false

			// Informs clients
			informClientsAbtNewRole(newTail)
		}
	} else if msg.From == SERVERNODE && msg.Type == PROCESSED_TRANS_STATUS {
		LogStatement.Printf("Received Processed Transaction Status %+v \n", msg.NodeStatus)
		predecessor := serverConfigs[msg.Bank][msg.NodeStatus.FailedServerIndex-1]
		newSuccessor := serverConfigs[msg.Bank][msg.NodeStatus.FailedServerIndex+1]
		LogStatement.Printf("Removed this Node due to failure of node %+v \n", serverConfigs[msg.Bank][msg.NodeStatus.FailedServerIndex])
		copy(serverConfigs[msg.Bank][msg.NodeStatus.FailedServerIndex:], serverConfigs[msg.Bank][msg.NodeStatus.FailedServerIndex+1:])
		serverConfigs[msg.Bank] = serverConfigs[msg.Bank][:len(serverConfigs[msg.Bank])-1]
		sendNodeFailureInfoToPredecessor(msg.NodeStatus, predecessor, newSuccessor)

	/*} else if msg.From == NEW_INTERMEDIATE && msg.Type == CLAIM_ROLE {
		LogStatement.Printf("Received New Intermediate confirmation message %+v \n")
		sConfig := ServerConfig{}
		sConfig = msg.SConfig

		var predecessor ServerConfig
		var successor ServerConfig

		for index := 0; index < len(serverConfigs[msg.Bank]); index++ {
			if sConfig.ServerId == serverConfigs[msg.Bank][index].ServerId {
				predecessor = serverConfigs[msg.Bank][index-2]
				successor = serverConfigs[msg.Bank][index]
				LogStatement.Printf("Removed on Node due to failure of node %+v \n", serverConfigs[msg.Bank][index-1])
				copy(serverConfigs[msg.Bank][index-1:], serverConfigs[msg.Bank][index:])
				serverConfigs[msg.Bank] = serverConfigs[msg.Bank][:len(serverConfigs[msg.Bank])-1]
				break
			}
		}

		sendNewSuccessorinfoToPredecessor(predecessor, successor)
		*/
	}
}

// Check servers are up or not
func checkServers(Bank string) {
	for index := 0; index < len(serverConfigs[Bank]); index++ {
		sConfig := serverConfigs[Bank][index]
		if sConfig.receivedPing == false {
			LogStatement.Printf("Bank %s, Server Id %s and designation %s found dead\n",
				sConfig.Bank, sConfig.ServerId, sConfig.ServerType)
			if sConfig.ServerType == "TAIL" {
				handleTailFailure(index, Bank)
				if newNodeJoinInProgress == true {
					newNode := serverConfigs[Bank][len(serverConfigs[Bank])-1]
					presenttail := serverConfigs[Bank][len(serverConfigs[Bank])-2]
					sendTailAbtNewServer(newNode, presenttail)
				}
			} else if sConfig.ServerType == "HEAD" {
				handleHeadFailure(index, Bank)
			} else if sConfig.ServerType == "INTERMEDIATE" {
				handleIntermediateFailure(index, Bank)
			} else if sConfig.ServerType == "NEW_NODE" {
				handleNewNodeFailure(index, Bank)
			}
		}
	}

	resetServers(Bank)
	return
}

func handleNewNodeFailure(index int, Bank string) {
	if len(serverConfigs[Bank]) > index-1 {
		oldTail := serverConfigs[Bank][index-1]
		// failedNode := serverConfigs[Bank][index]
		copy(serverConfigs[Bank][index:], serverConfigs[Bank][index+1:])
		serverConfigs[Bank] = serverConfigs[Bank][:len(serverConfigs[Bank])-1]

		// Inform old tail to regain as tail
		informNewNodeFailureToOldTail(oldTail)
	}
}

func handleIntermediateFailure(index int, Bank string) {
	if len(serverConfigs[Bank]) > index+1 {
		newIntermediateSConfig := serverConfigs[Bank][index+1]
		predecessor := serverConfigs[Bank][index-1]
		failedServerIndex := index

		// Inform Next Node
		informNodeFailureToSuccessor(newIntermediateSConfig, failedServerIndex, predecessor)
	}
}

func handleHeadFailure(index int, Bank string) {
	if len(serverConfigs[Bank]) > index+1 {
		serverConfigs[Bank][index+1].ServerType = "HEAD"
		sConfig := serverConfigs[Bank][index+1]

		time.Sleep(10 * time.Second)

		// Inform Next Node
		informNewRoleToNode(sConfig)

		// Inform other bank servers
		informOtherBanks(sConfig)

		// Informs clients
		informClientsAbtNewRole(sConfig)

		copy(serverConfigs[Bank][index:], serverConfigs[Bank][index+1:])
		// serverConfigs[Bank][len(serverConfigs[Bank])-1] = 0 // or the zero value of T
		serverConfigs[Bank] = serverConfigs[Bank][:len(serverConfigs[Bank])-1]
	}
}

func handleTailFailure(index int, Bank string) {
	//if (len(serverConfigs[Bank]) <= index-1) {
	serverConfigs[Bank][index-1].ServerType = "TAIL"
	sConfig := serverConfigs[Bank][index-1]

	// Inform Next Node
	informNewRoleToNode(sConfig)

	// Inform other bank servers
	informOtherBanks(sConfig)

	// Informs clients
	informClientsAbtNewRole(sConfig)
	//}

	copy(serverConfigs[Bank][index:], serverConfigs[Bank][index+1:])
	// serverConfigs[Bank][len(serverConfigs[Bank])-1] = nil // or the zero value of T
	serverConfigs[Bank] = serverConfigs[Bank][:len(serverConfigs[Bank])-1]
}

// Inform clients about new Head/ Tail
func informClientsAbtNewRole(sConfig ServerConfig) {
	newConfig := NewConfig{}
	if sConfig.ServerType == "HEAD" {
		newConfig.Role = sConfig.ServerType
		newConfig.UdpAddress = sConfig.UdpUpdateAddress
		newConfig.Bank = sConfig.Bank
	} else if sConfig.ServerType == "TAIL" {
		newConfig.Role = sConfig.ServerType
		newConfig.UdpAddress = sConfig.UdpQueryAddress
		newConfig.Bank = sConfig.Bank
	}

	for i := 0; i < len(clientConfigs[sConfig.Bank]) ; i++ {
		sendNewRoleMsgToClient(newConfig, clientConfigs[sConfig.Bank][i])
	}
}

// Inform about failures to other banks
func informOtherBanks(sConfig ServerConfig) {
	newConfig := NewConfig{}
	newConfig.Role = sConfig.ServerType
	newConfig.TcpAddress = sConfig.TcpAddress
	newConfig.Bank = sConfig.Bank

	fmt.Println("New Tail is ", sConfig)

	for i := 0; i < len(banks["BANKS"]); i++ {
		bankName := banks["BANKS"][i].Bank
		if bankName != sConfig.Bank {
			fmt.Println("bankName :: ", bankName, "   sConfig.Bank :: ", sConfig.Bank)
			for j := 0; j < len(serverConfigs[bankName]); j++ {
				otherBank := serverConfigs[bankName][j]
				sendNewRoleMsgToOtherBankServer(newConfig, otherBank)
			}
		} else {
			fmt.Println("bankName :: ", bankName, "   sConfig.Bank :: ", sConfig.Bank)
		}
	}

	LogStatement.Printf("Sent new role message to other bank servers successfully %+v \n", newConfig)
}

func sendNewRoleMsgToOtherBankServer(newConfig NewConfig, otherBank ServerConfig) {
	// LogStatement.Printf("Sending new config to other bank server... %+v ", otherBank.TcpAddress)

	conn, err := net.Dial("tcp", otherBank.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to bank %s server error :  %s \n", otherBank.TcpAddress, err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.NewConfigMsg = newConfig
	msg.Type = OTHER_BANK_RELATED
	msg.SubType = NEW_ROLE

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master New Role Message to other bank server Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to to other bank server error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master sent New Role Status to %s bank %s server successfully : \n", otherBank.Bank, otherBank.ServerId, msg.NewConfigMsg)
}

func sendNewRoleMsgToClient(newConfig NewConfig, clientConfig ClientConfig) {
	LogStatement.Printf("Sending new config to client %s (%s)", clientConfig.ClientId, clientConfig.UDPMasterAddress)

	conn, err := net.Dial("udp", clientConfig.UDPMasterAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to Client %s error :  %s \n", clientConfig.ClientId, err)
	}
	defer conn.Close()

	message, err := json.Marshal(newConfig)
	if err != nil {
		LogStatement.Printf("Master New Role Message to client Json marshall failed : %s \n", err)
	}

	_, udpSentErr := conn.Write([]byte(message))
	if udpSentErr != nil {
		LogStatement.Printf("Master write to Client %s error : %s \n", clientConfig.ClientId, udpSentErr)
	}

	conn.Close()
	LogStatement.Printf("Master sent New Role Status to Client %s successfully : %+v \n", clientConfig.ClientId, newConfig)
}

//Informs new role to next node
func informNewRoleToNode(sConfig ServerConfig) {
	conn, err := net.Dial("tcp", sConfig.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to New Node Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.NewRoleMsg.Role = sConfig.ServerType
	msg.Type = NEW_ROLE
	msg.Bank = sConfig.Bank

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master New Role Message Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to New Node error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master sent New Role Status %+v to New Node %s successfully : \n", msg.NewRoleMsg, sConfig.TcpAddress)
}

// Informs tail about new node joining
func sendTailAbtNewServer(newNode ServerConfig, tail ServerConfig) {
	conn, err := net.Dial("tcp", tail.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to Tail Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.Type = NODE_JOIN
	msg.Bank = tail.Bank
	msg.SConfig = newNode

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master Tail Message Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to Tail error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master informed new node information to old tail : %+v \n", msg.SConfig)
}

func informNewNodeFailureToOldTail(oldTail ServerConfig) {
	conn, err := net.Dial("tcp", oldTail.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to Old Tail Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.Type = NEW_NODE_FAILURE
	msg.Bank = oldTail.Bank
	// msg.SConfig = failedNode
	msg.NewRoleMsg.Role = "TAIL"

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master Message to to Old Tail Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to to Old Tail error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master informed new node failure information to Old Tail \n")
}

// Informs successor about its predecessor failure
func informNodeFailureToSuccessor(newIntermediateSConfig ServerConfig, failedServerIndex int, predecessor ServerConfig) {
	conn, err := net.Dial("tcp", newIntermediateSConfig.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to Successor Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.Type = PREDECESSOR_FAILURE
	msg.Bank = newIntermediateSConfig.Bank
	msg.SConfig = predecessor
	msg.NodeStatus.FailedServerIndex = failedServerIndex

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master Successor Message Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to Tail error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master informed node failure information to successor \n")
}

func sendNodeFailureInfoToPredecessor(nodeStatus ServerNodeStatus, predecessor ServerConfig, newSuccessor ServerConfig) {
	conn, err := net.Dial("tcp", predecessor.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to Successor Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.Type = SUCCESSOR_FAILURE
	msg.Bank = predecessor.Bank
	msg.NodeStatus = nodeStatus
	msg.SConfig = newSuccessor

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master Successor Message Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to Tail error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master informed successor failure and new successor node status to predecessor : %+v \n", msg.SConfig)
}

func sendNewSuccessorinfoToPredecessor(predecessor ServerConfig, successor ServerConfig) {
	conn, err := net.Dial("tcp", predecessor.TcpAddress)
	if err != nil {
		LogStatement.Printf("Master Dial to predecessor Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = MASTER
	msg.Type = NEW_SUCCESSOR_INFO
	msg.Bank = successor.Bank
	msg.SConfig = successor

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Master predecessor Message Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Master write to Tail error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Master sent new successor information to predecessor : %+v \n", msg.SConfig)
}

// Resets all server status of pings
func resetServers(Bank string) {
	for i := 0; i < len(serverConfigs[Bank]); i++ {
		serverConfigs[Bank][i].receivedPing = false
	}
}

// Triggers to check servers at regular intervals
func TimerTick(Bank string, timeOut int) {
	LogStatement.Printf("Started Timer thread for Bank : %s \n", Bank)
	ticker := time.NewTicker(time.Duration(timeOut) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				checkServers(Bank)
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

// Setting Logger
func setLogger() {
	fileName := "Master.log"
	file, err := os.OpenFile("../logs/" + fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Failed to open log file : %s \n", err)
	}

	multi := io.MultiWriter(file, os.Stdout)

	prefix := "MASTER : "
	LogStatement = log.New(multi,
		prefix,
		log.Ldate|log.Ltime) // |log.Lshortfile) To include line numbers of logged line
	LogStatement.Printf("Master Started Logging...\n")
}
