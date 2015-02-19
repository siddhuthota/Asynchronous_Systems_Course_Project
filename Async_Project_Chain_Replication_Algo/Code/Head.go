package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

// Reply structure to send response to client
type Reply struct {
	ReqID      string
	AccountNum string
	Outcome    int
	Balance    float64
}

// Query structure to receive get balance request
type Query struct {
	ReqID      string
	AccountNum string
}

// Account structure to store the details of client accounts
type Account struct {
	AccountNum string
	Balance    float64
}

// Update structure to receive update balance request
type Update struct {
	UpdateType     int // 0 - Deposit, 1 - Withdraw, 2 - Transfer
	ReqID          string
	AccountNum     string
	Amount         float64
	DestAccountNum string
	DestBank       string
	SrcBank        string
}

// Transaction structure to store client requests
type Transaction struct {
	RequestType    int // 0 - Deposit, 1 - Withdraw, 2 - Transfer
	ReqID          string
	AccountNum     string
	Balance        float64
	Amount         float64
	Outcome        int
	ClientId       string
	DestAccountNum string
	DestBank       string
	SrcBank        string
}

// config structure to store config values of all servers nodes
type ServerConfig struct {
	ServerId           string
	TcpAddress         string
	UdpQueryAddress    string
	UdpUpdateAddress   string
	ServerType         string
	Bank               string
	StartDelay         int
	LifeTimeMsgs       int
	NoOfClients        int
	SMinusAbort        bool
	NewNodeFailure     bool
	NewNodeTailFailure bool
	SPlusAbort         bool
	TransferReqFailure bool
	TransferReqSendFailure bool
	LifeTime           string
	MasterPingTCP      string
	MasterMiscTCP      string
	PingDelay          int
	OtherBanks         []OtherBankInfo
}

// Global structure of message
type Message struct {
	From                 int
	Type                 int
	SubType              int
	Bank                 string
	UpdateReq            Update
	SConfig              ServerConfig
	PingMsg              Ping
	NewRoleMsg           NewRole
	NewConfigMsg 				 NewConfig
	TrnsfrProcessedTrans TransferProcessedTrans
	Client               ClientDetails
	NodeStatus           ServerNodeStatus
	AckToReq             Req
	RequestReply         Reply
}

// Stores client address of the request
type ClientDetails struct {
	ClientAddress string
}

// Stores new role message
type NewRole struct {
	Role string
}

type NewConfig struct {
	Role       string
	UdpAddress string
	TcpAddress string
	Bank       string
}

// Stores Server Node Status
type ServerNodeStatus struct {
	ProcessedTransCount int
	AccountsCount       int
	FailedServerIndex   int
}

// To store Client configurations
type ClientConfig struct {
	ClientId                string
	UDPListenAddress        string
	UdpUpdateReqSentAddress string
	Bank                    string
}

// To send ack to the predecessor
type Req struct {
	ReqID string
}

// To transfer Accounts and requests
type TransferProcessedTrans struct {
	ProcessedTrans map[string]Transaction
	Accounts       map[string]Account
}

type OtherBankInfo struct {
	Bank           string
	HeadTcpAddress string
	TailTcpAddress string
}

// Enum to maintain results of queries posed by client
const (
	Processed               = 0
	InconsistentWithHistory = -1
	InsufficientFunds       = 1
)

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
	SUCCESSOR        = 9
	OTHER_BANK_TAIL  = 10
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
	ACK_TO_REQ             = 31
	PROCESS_REQ            = 32
	NEW_NODE_FAILURE       = 33
	TAIL_FAILURE           = 34
	HEAD_FAILURE           = 35
	TRANSFER_REQ           = 36
	TRANSFER_ACK           = 37
	OTHER_BANK_RELATED     = 38
)

// Sample accounts
var userAccounts = map[string]Account{
	"ICICI801": Account{"ICICI801", 20.00},
	"HDFC801": Account{"HDFC801", 122.00},
	"BOA801": Account{"BOA801", 12.00},
}

// Ping structure to send ping to master
type Ping struct {
	ServerId string
	Bank     string
}

// stores all the processed requests
var processedTrans = map[string]Transaction{}

// Global Variables to store all the required values
var NodeDesignation string
var MyTCP string
var MyUDPUpdate string
var MyUDPQuery string
var Predecessor string
var Successor string
var ServerId string
var MasterPingTCP string
var MasterMiscTCP string
var Bank string
var MyConfig ServerConfig
var ReceivedMsgs int
var SentMsgs int

// Map that stores all configurations of server nodes
var allServerConfigs map[string][]ServerConfig

// Map that stores all configurations of client nodes
var allClientConfigs map[string][]ClientConfig

// Reqs which are waiting for acks that they are processed by tail
var sentReqIds []string

// Logger
var LogStatement *log.Logger

// Client servers tag
const CLIENT_SERVERS_TAG = "CLIENT_SERVERS"

var Abort bool

// This function handles request received by predecessor if current noce is tail or intermediate
func handlePredecessorReq(msg Message) {

	/*if (NodeDesignation == "TAIL" && msg.UpdateReq.UpdateType == 2) {
		isReqValid := IsTransferReqvalid(msg.UpdateReq)
		if isReqValid == false {
			LogStatement.Printf("Received transfer req that is already sent to other bank head \n")
			return
		}
	}*/

	// reply := processPredecessorRequest(msg.UpdateReq, msg.Client.ClientAddress)

	// Sends to successor or client based on node designation
	if NodeDesignation == "INTERMEDIATE" {
		processPredecessorRequest(msg.UpdateReq, msg.Client.ClientAddress)
		sentReqIds = append(sentReqIds, msg.UpdateReq.ReqID)
		sendReqToSuccessor(msg.Client.ClientAddress, msg.UpdateReq, Successor)
	} else if NodeDesignation == "TAIL" {
		if msg.UpdateReq.UpdateType == 2 {
			if msg.UpdateReq.DestBank != Bank {
				isPresent := IsPresentInSentQ(msg.UpdateReq)
				if isPresent == false {
					_, present := processedTrans[msg.UpdateReq.ReqID]
					reply := processPredecessorRequest(msg.UpdateReq, msg.Client.ClientAddress)
					if reply.Outcome != 0 {
						/*reqId := reply.ReqID
						for i := 0; i < len(sentReqIds); i++ {
							if reqId == sentReqIds[i] {
								copy(sentReqIds[i:], sentReqIds[i+1:])
								sentReqIds = sentReqIds[:len(sentReqIds)-1]
								break
							}
						}*/
						sendReplyToClient(msg.Client.ClientAddress, reply)
						sendAckToPredecessor(Req{msg.UpdateReq.ReqID}, Predecessor)
					} else {
						if present == false {
							sendTransferReq(msg.UpdateReq)
						} else {
							sendReplyToClient(msg.Client.ClientAddress, reply)
							sendAckToPredecessor(Req{msg.UpdateReq.ReqID}, Predecessor)
						}
					}
				} else {
					LogStatement.Printf("Sent this transfer request to destination bank and waiting for reply %+v \n", msg.UpdateReq)
				}
			} else {
				reply := processPredecessorRequest(msg.UpdateReq, msg.Client.ClientAddress)
				srcBank := msg.UpdateReq.SrcBank
				sendTransferAck(reply, srcBank)
				sendAckToPredecessor(Req{msg.UpdateReq.ReqID}, Predecessor)
			}
		} else {
			reply := processPredecessorRequest(msg.UpdateReq, msg.Client.ClientAddress)
			sendReplyToClient(msg.Client.ClientAddress, reply)
			sendAckToPredecessor(Req{msg.UpdateReq.ReqID}, Predecessor)
		}
	}
}

func IsPresentInSentQ(update Update) (isPresent bool) {
	for i := 0; i < len(sentReqIds); i++ {
		if sentReqIds[i] == update.ReqID {
			isPresent = true
			break
		}
	}
	return
}

func sendTransferReq(updateReq Update) {
	destHeadTcp := ""

	for i := 0; i < len(MyConfig.OtherBanks); i++ {
		if MyConfig.OtherBanks[i].Bank == updateReq.DestBank {
			destHeadTcp = MyConfig.OtherBanks[i].HeadTcpAddress
			break
		}
	}

	sentReqIds = append(sentReqIds, updateReq.ReqID)
	sendTransferReqToDestBank(updateReq, destHeadTcp)
}

func sendTransferAck(reply Reply, srcBank string) {
	destTailTcp := ""

	for i := 0; i < len(MyConfig.OtherBanks); i++ {
		if MyConfig.OtherBanks[i].Bank == srcBank {
			destTailTcp = MyConfig.OtherBanks[i].TailTcpAddress
			break
		}
	}

	newReply := Reply{}
	newReply.Outcome = reply.Outcome
	newReply.AccountNum = reply.AccountNum
	newReply.ReqID = reply.ReqID

	sendTransferAckToDestBank(newReply, destTailTcp)
}

// To get client UDP listening address
func getClientUDPListenAddr(clientAddr string) (cleintUdpListen string) {
	for i := 0; i < len(allClientConfigs[CLIENT_SERVERS_TAG]); i++ {
		config := allClientConfigs[CLIENT_SERVERS_TAG][i]
		if config.UdpUpdateReqSentAddress == clientAddr {
			cleintUdpListen = config.UDPListenAddress
			return
		}
	}
	return
}

// Processes predecessor requests
func processPredecessorRequest(update Update, clientId string) Reply {
	// update := Update{processReq.RequestType, processReq.ReqID, processReq.AccountNum, processReq.Amount}
	reply := processClientUpdateRequest(update, clientId)
	return reply
}

// Reads and stores configuration values from config file "config.json"
func readConfigValues() {
	bankName := os.Args[1]
	fileIndex := os.Args[2]
	/*if e != nil {
		fmt.Printf("Server index input arguments error : %s \n", e)
	}*/

	file, fileOpenError := os.OpenFile("../Config/Banks/"+bankName+"/Server_Config_"+fileIndex+".json", os.O_RDWR, 0666)
	allServerConfigs = make(map[string][]ServerConfig, 0)
	fileOpenError = json.NewDecoder(file).Decode(&allServerConfigs)
	if fileOpenError != nil {
		fmt.Printf("Server Config File Open Error : %s \n", fileOpenError)
	}
	// LogStatement.Println("Loaded Configurations :: ", configurations)

	serverIndex, e := strconv.Atoi(os.Args[3])
	if e != nil {
		fmt.Printf("Server index input arguments error : %s \n", e)
	}

	serversName := "BANK_SERVERS"
	MyConfig = allServerConfigs[serversName][serverIndex]
	fmt.Printf("My Configuration :: %+v \n", MyConfig)
	NodeDesignation = MyConfig.ServerType
	MyTCP = MyConfig.TcpAddress
	MyUDPQuery = MyConfig.UdpQueryAddress
	MyUDPUpdate = MyConfig.UdpUpdateAddress
	ServerId = MyConfig.ServerId
	Bank = MyConfig.Bank
	MasterPingTCP = MyConfig.MasterPingTCP
	MasterMiscTCP = MyConfig.MasterMiscTCP

	// Populates predecessor and successor based on node designation
	if MyConfig.ServerType == "HEAD" {
		Successor = (allServerConfigs[serversName][serverIndex+1]).TcpAddress
	} else if MyConfig.ServerType == "INTERMEDIATE" {
		Predecessor = (allServerConfigs[serversName][serverIndex-1]).TcpAddress
		Successor = (allServerConfigs[serversName][serverIndex+1]).TcpAddress
	} else if MyConfig.ServerType == "TAIL" {
		Predecessor = (allServerConfigs[serversName][serverIndex-1]).TcpAddress
	}

	// Load client configurations
	loadClientConfigValues(bankName)
	ReceivedMsgs = 1
	SentMsgs = 1
	Abort = false
}

// Load client configurations
func loadClientConfigValues(bankName string) {
	file, fileOpenError := os.OpenFile("../Config/Banks/"+bankName+"/Bank_Clients_Info"+".json", os.O_RDWR, 0666)
	allClientConfigs = make(map[string][]ClientConfig, 0)
	fileOpenError = json.NewDecoder(file).Decode(&allClientConfigs)
	if fileOpenError != nil {
		fmt.Printf("Unable to read bank_clients.json file : %s \n", fileOpenError)
	}

	// fmt.Printf("All Client Configurations :: %s \n", allClientConfigs)
}

// Main function
func main() {
	// setLogger() // If you keep here u are not getting serverId as config values are not loaded

	readConfigValues()
	setLogger()
	LogStatement.Printf("Started Logging...\n")
	LogStatement.Printf("Server starts after %d seconds", MyConfig.StartDelay)
	startServerDelay(MyConfig.StartDelay)

	ch := make(chan int)
	v := <-ch
	fmt.Printf("V = \n", v)
}

// Processes client update request
func processClientUpdateRequest(update Update, clientId string) Reply {
	// Validates account number, if not exists then creates one
	validateAccount(update.AccountNum)

	// Validates request and processes request
	// If present in request queue already then send appropriate reply
	// else process and returns result (reply)
	isReqValid, reply := validateRequest(update)
	if isReqValid == true {
		if update.UpdateType == 0 {
			LogStatement.Printf("Depositing the amount into account...\n")
			reply = depositBalance(update, clientId)
		} else if update.UpdateType == 1 {
			LogStatement.Printf("Withdrawing the amount from account...\n")
			reply = withDrawBalance(update, clientId)
		} else if update.UpdateType == 2 {
			if update.DestBank != Bank {
				LogStatement.Printf("Transfering the amount from account...\n")
				update.SrcBank = Bank
				reply = withDrawBalance(update, clientId)
			} else {
				LogStatement.Printf("Transfering the amount to account...\n")
				reply = depositBalance(update, clientId)
			}
		}

	} else {
		// LogStatement.Println("ReqID matched with previous requests ?", reply)
	}

	return reply
}

// Checks and sends returns reply
func getBalance(query Query) Reply {
	bal := (userAccounts[query.AccountNum]).Balance
	reply := Reply{query.ReqID, query.AccountNum, Processed, bal}
	return reply
}

// Validates account number, if not exists then creates one
func validateAccount(accountNum string) {
	_, present := userAccounts[accountNum]
	if present == false {
		userAccounts[accountNum] = Account{accountNum, 0}
		// fmt.Println("Added Account :: ", userAccounts[accountNum])
	}
}

func handleMasterReq(msg Message) {
	if msg.Type == NEW_ROLE {
		if msg.NewRoleMsg.Role == "HEAD" {
			LogStatement.Printf("Past Node designation : %s ", NodeDesignation)
			LogStatement.Printf("Received New Role message from Master : %s", msg.NewRoleMsg.Role)
			if NodeDesignation == "TAIL" {
				NodeDesignation = "HEAD&TAIL"
			} else {
				NodeDesignation = msg.NewRoleMsg.Role
			}
			LogStatement.Printf("Present Node designation : %s ", NodeDesignation)
			go listenUpdateUDPConn()
		} else if msg.NewRoleMsg.Role == "TAIL" {
			LogStatement.Printf("Past Node designation : %s ", NodeDesignation)
			LogStatement.Printf("Received New Role message from Master : %s", msg.NewRoleMsg.Role)
			if NodeDesignation == "HEAD" {
				NodeDesignation = "HEAD&TAIL"
			} else {
				NodeDesignation = msg.NewRoleMsg.Role
			}
			LogStatement.Printf("Present Node designation : %s ", NodeDesignation)
			go listenQueryUDPConn()
		}
	} else if msg.Type == PREDECESSOR_FAILURE {
		LogStatement.Printf("Received from Master about Predecessor Node failure and new Predecessor info %+v %+v \n", msg.NodeStatus, msg.SConfig)
		nodeStatus := ServerNodeStatus{len(processedTrans), len(userAccounts), msg.NodeStatus.FailedServerIndex}
		Predecessor = msg.SConfig.TcpAddress
		LogStatement.Printf("Updated new Predecessor TCP Address %+v \n", Predecessor)
		sendProcessedTransStatusToMaster(nodeStatus)
	} else if msg.Type == SUCCESSOR_FAILURE {
		LogStatement.Printf("Received from Master about Successor failure and New Successor information and status: %+v %+v \n", msg.NodeStatus, msg.SConfig)
		Successor = msg.SConfig.TcpAddress
		LogStatement.Printf("Updated new Successor TCP Address %+v \n", Successor)
		sendProcessedTransStatusToNewSuccessor(msg.NodeStatus, msg.SConfig)
	} else if msg.Type == NODE_JOIN {
		LogStatement.Printf("Received from Master about New Node : %+v \n", msg.SConfig)
		sendAllProcessedTransToNewTail(msg.SConfig.TcpAddress)
		NodeDesignation = "INTERMEDIATE"
		Successor = msg.SConfig.TcpAddress
		LogStatement.Printf("Changed My role from TAIL to %s ", NodeDesignation)
	} else if msg.Type == NEW_NODE_FAILURE {
		LogStatement.Printf("Received from Master about New Node failure")
		NodeDesignation = msg.NewRoleMsg.Role
		Successor = ""
		LogStatement.Printf("Changed My role from INTERMEDIATE to %s ", NodeDesignation)
	} else if msg.Type == OTHER_BANK_RELATED {
		handleOtherBankRelatedMsgs(msg)
	}
}

func handleOtherBankRelatedMsgs(msg Message) {
	if msg.SubType == NEW_ROLE {
		LogStatement.Printf("Received other bank new node role from Master %+v \n", msg.NewConfigMsg)
		newConfig := msg.NewConfigMsg
		for i := 0; i < len(MyConfig.OtherBanks); i++ {
			if MyConfig.OtherBanks[i].Bank == newConfig.Bank {
				if newConfig.Role == "HEAD" {
					MyConfig.OtherBanks[i].HeadTcpAddress = newConfig.TcpAddress
					LogStatement.Printf("Received & Updated other bank new head %s from Master \n", MyConfig.OtherBanks[i].HeadTcpAddress)
					if NodeDesignation == "TAIL" {
						sendTransferReqsIfAnyToNewHead(newConfig.TcpAddress)
					}
				} else if newConfig.Role == "TAIL" {
					MyConfig.OtherBanks[i].TailTcpAddress = newConfig.TcpAddress
					LogStatement.Printf("Received & Updated other bank new tail %s from Master \n", MyConfig.OtherBanks[i].TailTcpAddress)
				}
			}
		}
	}
}

func sendTransferReqsIfAnyToNewHead(destHeadTcp string) {
	ifAnyTransferReqsPending := false
	// fmt.Println("Transfer reqs present :: ", len(sentReqIds))
	if len(sentReqIds) > 0 {
		for i := 0 ; i < len(sentReqIds); i++ {
			ifAnyTransferReqsPending = true
			if processedTrans[sentReqIds[i]].RequestType == 2 {
				transaction := processedTrans[sentReqIds[i]]
				update := Update{transaction.RequestType, transaction.ReqID, transaction.AccountNum,
						transaction.Amount, transaction.DestAccountNum, transaction.DestBank, Bank}
				LogStatement.Printf("Resending the transfer request to new head %s... %+v \n", destHeadTcp, update)
				sendTransferReqToDestBank(update, destHeadTcp)
			}
		}
	}

	if ifAnyTransferReqsPending == false {
		LogStatement.Printf("No Transfer requests pending to be sent... \n")
	}
}

func handleOldTailReq(transferMsg TransferProcessedTrans) {
	// Get all the processed Transactions from old tail
	processedTrans = transferMsg.ProcessedTrans
	userAccounts = transferMsg.Accounts

	NodeDesignation = "TAIL"
	LogStatement.Printf("Changed My role from NEWNODE to %s \n", NodeDesignation)

	// Assign new predecessor
	claimAsNewTailToMaster()
	go listenQueryUDPConn()
}

// Validates the update request sent by clients, whether the new request is already processed or not
func validateRequest(update Update) (isReqValid bool, reply Reply) {
	isReqValid = true
	reply = Reply{}

	request, present := processedTrans[update.ReqID]
	LogStatement.Printf("ReqID %s Present in processed requests? : %t", update.ReqID, present)
	if present == true {
		// If Update type of request not matches then returns InconsistentWithHistory else Processed
		if request.RequestType != update.UpdateType {
			isReqValid = false
			reply = Reply{update.ReqID, update.AccountNum, InconsistentWithHistory, (userAccounts[update.AccountNum]).Balance}
		} else {
			isReqValid = false
			reply = Reply{update.ReqID, update.AccountNum, request.Outcome, (userAccounts[update.AccountNum]).Balance}
		}
	}
	return
}

// Deposits amount in the account requested by client
func depositBalance(update Update, clientId string) Reply {
	// fmt.Println("Received acct for deposit :: ", update.AccountNum, update.ReqID)
	account := (userAccounts[update.AccountNum])
	account.Balance = account.Balance + update.Amount
	userAccounts[update.AccountNum] = account
	LogStatement.Printf("Deposited balance successfully...\n")
	reply := Reply{update.ReqID, update.AccountNum, Processed, userAccounts[update.AccountNum].Balance}
	saveTransaction(update, account, reply, clientId)
	return reply
}

// Withdrws amount from the account specified by client
// returns processed with current balance if enough funds exist
// else returns InsufficientFunds along with current balance
func withDrawBalance(update Update, clientId string) Reply {
	reply := Reply{}

	if (userAccounts[update.AccountNum]).Balance >= update.Amount {
		account := (userAccounts[update.AccountNum])
		// fmt.Println("Account is :: %+v", account)
		account.Balance = account.Balance - update.Amount
		userAccounts[update.AccountNum] = account
		if update.UpdateType == 1 {
			LogStatement.Printf("Withdrawn balance successfully...\n")
		} else if update.UpdateType == 2 {
			LogStatement.Printf("For Transfer, Withdrawn balance successfully...\n")
		}
		reply = Reply{update.ReqID, update.AccountNum, Processed, (userAccounts[update.AccountNum]).Balance}
		saveTransaction(update, account, reply, clientId)
	} else {
		if update.UpdateType == 1 {
			LogStatement.Printf("Withdrawn balance failure due to insufficient funds...\n")
		} else if update.UpdateType == 2 {
			LogStatement.Printf("For Transfer, Withdrawn balance failure due to insufficient funds...\n")
		}
		account := (userAccounts[update.AccountNum])
		reply = Reply{update.ReqID, update.AccountNum, InsufficientFunds, (userAccounts[update.AccountNum]).Balance}
		saveTransaction(update, account, reply, clientId)
	}

	return reply
}

// Saves requests that are processed successfully by server in requests map
func saveTransaction(update Update, account Account, reply Reply, clientId string) {
	transaction := Transaction{}
	transaction.ReqID = update.ReqID
	transaction.RequestType = update.UpdateType
	transaction.Balance = account.Balance
	transaction.AccountNum = account.AccountNum
	transaction.Outcome = reply.Outcome
	transaction.Amount = update.Amount
	transaction.ClientId = clientId
	transaction.DestAccountNum = update.DestAccountNum
	transaction.DestBank = update.DestBank
	transaction.SrcBank = update.SrcBank
	processedTrans[update.ReqID] = transaction
	LogStatement.Printf("Processed Transaction saved in Processed Transaction map... %+v", transaction)
}

func populateMessage(clientAddr string, update Update) Message {
	msg := Message{}
	msg.From = SERVERNODE
	msg.UpdateReq = update
	msg.Client.ClientAddress = clientAddr
	return msg
}

// Logger setting
func setLogger() {
	fileName := "Server_" + ServerId + ".log"
	file, err := os.OpenFile("../logs/"+fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Failed to open log file : %s \n", err)
	}

	multi := io.MultiWriter(file, os.Stdout)

	prefix := "SERVER " + ServerId + ": "
	LogStatement = log.New(multi,
		prefix,
		log.Ldate|log.Ltime) // |log.Lshortfile) To include line numbers of logged line
	LogStatement.Printf("Logger set successfully...\n")
}

// Master part
// Sends ping to master
func sendPingToMaster(delay int) {
	ticker := time.NewTicker(time.Duration(delay) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				if ((SentMsgs < MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "sent") ||
					(ReceivedMsgs < MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "received") ||
					(MyConfig.LifeTime == "unbounded")) && (Abort != true) {
					sendTCPPingToMaster()
				} else {
					LogStatement.Printf("Server Failed, Stopped server activities as it reaches maximum limit of %s messages", MyConfig.LifeTime)
					close(quit)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func sendTCPPingToMaster() {

	ping := Ping{ServerId, Bank}
	msg := Message{}
	msg.Type = PING
	msg.PingMsg = ping
	msg.Bank = Bank

	sendTCPMessage(MasterPingTCP, msg, "Server to Master Ping")
}

func sendTransferReqToDestBank(updateReq Update, destBankHeadTcp string) {
	conn, err := net.Dial("tcp", destBankHeadTcp)
	if err != nil {
		LogStatement.Printf("Transfer req destination bank head Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = OTHER_BANK_TAIL
	msg.Type = PROCESS_REQ
	msg.SubType = TRANSFER_REQ
	msg.UpdateReq.SrcBank = Bank
	msg.UpdateReq = updateReq
	msg.Client.ClientAddress = MyTCP

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Transfer req destination bank head Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Transfer req destination bank head write error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Transfer req sent destination bank head %s successfully with UpdateReq  %+v to server %s \n", destBankHeadTcp, msg.UpdateReq, destBankHeadTcp)

	if MyConfig.TransferReqSendFailure == true {
		Abort = true
		// LogStatement.Printf("Sent Transfer request and failing...\n")
	}
}

func sendTransferAckToDestBank(reply Reply, destBankTailTcp string) {
	conn, err := net.Dial("tcp", destBankTailTcp)
	if err != nil {
		LogStatement.Printf("Transfer ack destination bank head Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = OTHER_BANK_TAIL
	msg.Type = PROCESS_REQ
	msg.SubType = TRANSFER_ACK
	msg.RequestReply = reply
	// msg.UpdateReq.SrcBank = Bank
	// msg.UpdateReq = updateReq
	msg.Client.ClientAddress = MyTCP

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Transfer ack destination bank head Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Transfer ack destination bank head write error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Transfer ack sent destination bank tail %s successfully with Reply : %+v \n", destBankTailTcp, msg.RequestReply)
}

func sendJoinReqToMaster() {
	conn, err := net.Dial("tcp", MasterMiscTCP)
	if err != nil {
		LogStatement.Printf("Node Join Request Dial to Master Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = NEW_NODE
	msg.Type = NODE_JOIN
	msg.SConfig = MyConfig

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Node Join Server to Master Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Node Join Server to Master write error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Node_Join Server request sent to Master successfully with configuration : %+v \n", msg.SConfig)
	// incrementSendMsgsCount()
}

// UDP Connections listening, Sending and Receiving

// This function sends reply to client, if current node designation is TAIL
func sendReplyToClient(clientAddress string, reply Reply) {
	clientUDPListenAddr := getClientUDPListenAddr(clientAddress)

	cConn, err3 := net.Dial("udp", clientUDPListenAddr)
	if err3 != nil {
		LogStatement.Printf("Resolving UDP client address : %s \n", err3)
	}
	defer cConn.Close()

	message, err := json.Marshal(reply)
	if err != nil {
		LogStatement.Printf("UDP client reply Json marshall failed : %s \n", err, message)
	}

	num, udpSentErr := cConn.Write(message)
	if udpSentErr != nil {
		LogStatement.Printf("UDP Client reply write failed : %s \n", udpSentErr, num)
	}

	LogStatement.Printf("Sent Seq %d : Update request reply %+v sent to client : %s \n", SentMsgs, reply, clientUDPListenAddr)
	incrementSendMsgsCount()
}

// Listens UDP server to receive UPDATE requests from client
func listenUpdateUDPConn() {
	LogStatement.Printf("Server UDP Update Server Started...\n")

	sPort := MyUDPUpdate // "127.0.0.1:8080"
	sAddr, err := net.ResolveUDPAddr("udp", sPort)
	if err != nil {
		LogStatement.Printf("Resolving UDP Update address failed : %s \n", err)
	}

	sConn, err := net.ListenUDP("udp", sAddr)
	if err != nil {
		LogStatement.Printf("Server UDP Update listening thread failed : %s \n", err)
	}
	defer sConn.Close()

	LogStatement.Printf("UDP Update listening on %s \n", sConn.LocalAddr().String())

	for {
		var update Update
		jsonBytes := make([]byte, 1024)
		n, cAddress, udpErr := sConn.ReadFromUDP(jsonBytes)

		if udpErr != nil {
			LogStatement.Printf("UDP server read failed : %s \n", udpErr, cAddress)
		}

		jsonErr := json.Unmarshal(jsonBytes[:n], &update)
		if jsonErr != nil {
			LogStatement.Printf("Json unmarshall failed : %s \n", jsonErr)
		}

		// Client address from where the update request has come
		clientAddr := cAddress.IP.String() + ":" + strconv.Itoa(cAddress.Port)

		LogStatement.Printf("Received seq %d : Received update request from client %+v", ReceivedMsgs, update)
		update.SrcBank = Bank
		incrementReceiveMsgsCount()
		reply := processClientUpdateRequest(update, clientAddr)
		LogStatement.Printf("Processed update request and outcome is reply %+v", reply)

		if (SentMsgs > MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "sent") ||
			(ReceivedMsgs > MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "received") || (Abort == true) {
			sConn.Close()
			LogStatement.Printf("Stopped UDP update server as it reached maximum %s messages limit", MyConfig.LifeTime)
			break
		}

		if update.UpdateType == 2 {
			update.SrcBank = Bank
		}

		// if designation is only head then forward to successor
		// else forward reply to client
		//if update.UpdateType = 2 {
			if NodeDesignation == "HEAD" {
				go sendReqToSuccessor(clientAddr, update, Successor)
			} else if NodeDesignation == "HEAD&TAIL" {
				if reply.Outcome != 0 {
					go sendReplyToClient(clientAddr, reply)
				} else {
					sendTransferReq(update)
				}
			}
		/*} else if update.UpdateType == 2 {
			if NodeDesignation == "HEAD" {
				go sendReqToSuccessor(clientAddr, update, Successor)
			} else if NodeDesignation == "HEAD&TAIL" {
				go sendReplyToClient(clientAddr, reply)
			}
		}*/
	}
}

// Listens UDP server to receive query (getBalanace) requests from client
func listenQueryUDPConn() {
	LogStatement.Printf("Server UDP Query Server Started...\n")

	sPort := MyUDPQuery
	sAddr, err := net.ResolveUDPAddr("udp", sPort)
	if err != nil {
		LogStatement.Printf("Resolving UDP Query address failed : %s \n", err)
	}

	sConn, err := net.ListenUDP("udp", sAddr)
	if err != nil {
		LogStatement.Printf("Server UDP Query listening thread failed : %s \n", err)
		if err.Error() == "listen udp "+sAddr.IP.String()+":"+strconv.Itoa(sAddr.Port)+": bind: address already in use" {
			return
		}
	}
	defer sConn.Close()

	LogStatement.Printf("UDP Query listening on %s \n", sConn.LocalAddr().String())

	for {
		var query Query
		jsonBytes := make([]byte, 1024)
		n, cAddress, udpErr := sConn.ReadFromUDP(jsonBytes)

		if udpErr != nil {
			LogStatement.Printf("UDP query server read failed : %s %s \n ", udpErr, cAddress)
		}

		// LogStatement.Printf("Received from client bytes\n", jsonBytes)
		jsonErr := json.Unmarshal(jsonBytes[:n], &query)
		if jsonErr != nil {
			LogStatement.Printf("UDP query server Json unmarshall failed : %s \n", jsonErr)
		}

		LogStatement.Printf("Receive Seq %d : UDP query server received request from client %+v", ReceivedMsgs, query)
		incrementReceiveMsgsCount()
		// Validates account number, if not exists then creates one
		validateAccount(query.AccountNum)

		// gets balance and replies to client
		reply := getBalance(query)
		message, err := json.Marshal(reply)
		if err != nil {
			LogStatement.Printf("UDP querver reply Json marshall failed : %s %s \n", err, message)
		}

		if (SentMsgs > MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "sent") ||
			(ReceivedMsgs > MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "received") || (Abort == true) {
			sConn.Close()
			LogStatement.Printf("Stopped UDP query server as it reached maximum %s messages limit", MyConfig.LifeTime)
			break
		}

		num, udpSentErr := sConn.WriteToUDP([]byte(message), cAddress)
		if udpSentErr != nil {
			LogStatement.Printf("UDP querver reply Json sent failed : %s %s \n", udpSentErr, num)
		}

		LogStatement.Printf("Sent Seq %d : Replied to client with reply %+v \n", SentMsgs, reply)
		incrementSendMsgsCount()
	}
}

// Sends process request to successor if node designation is HEAD or INTERMEDIATE
func sendReqToSuccessor(clientAddr string, update Update, successor string) {
	conn, err := net.Dial("tcp", successor)

	msg := Message{}
	msg.From = PREDECESSOR
	msg.Type = PROCESS_REQ
	msg.UpdateReq = update
	msg.Client.ClientAddress = clientAddr

	if update.UpdateType == 2 {
		msg.SubType = TRANSFER_REQ
	}

	if err != nil {
		LogStatement.Printf("TCP Client Connection error :  %s \n", err)
		return
	}
	defer conn.Close()

	LogStatement.Printf("TCP Client to successor connection established...\n")
	LogStatement.Printf("Sending process Request to successor  : %+v", msg.UpdateReq)

	jsonMessage, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("TCP Client to successor Json marshall failed : %s \n", err)
		return
	}

	_, tcpErr := conn.Write([]byte(jsonMessage))
	if tcpErr != nil {
		LogStatement.Printf("TCP Client to successor write error : %s \n", tcpErr)
		return
	}

	conn.Close()

	LogStatement.Printf("Sent Seq %d : Update Request written to Successor : %+v \n", SentMsgs, msg.UpdateReq)
	incrementSendMsgsCount()
}

// Listens to TCP server to get process requests from HEAD and INTERMEDIATE nodes
func listenTCP() {
	LogStatement.Printf("TCP Server Started...\n")
	ln, err := net.Listen("tcp", MyTCP)

	if err != nil {
		LogStatement.Printf("TCP Server listening thread failed : %s \n", err)
	}

	LogStatement.Printf("TCP Server Listening on... %s \n", MyTCP)

	for {
		conn, err := ln.Accept() // this blocks until connection or error
		if err != nil {
			LogStatement.Printf("TCP Server listening connection accept error : %s", err)
		}
		defer conn.Close() // Closes the connection at any cost even if any error occurred also

		if (SentMsgs > MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "sent") ||
		(ReceivedMsgs > MyConfig.LifeTimeMsgs && MyConfig.LifeTime == "received") || Abort == true {
			conn.Close()
			break
		}

		jsonBytes := make([]byte, 8192)
		n, jsonErr := conn.Read(jsonBytes)
		if jsonErr != nil {
			LogStatement.Printf("TCP connection Json read failed : %s \n", jsonErr)
		}

		var msg Message

		unmErr := json.Unmarshal(jsonBytes[:n], &msg)
		if err != nil {
			LogStatement.Printf("TCP connection transfermsg Json read failed : %s \n", unmErr)
		}

		if msg.From == MASTER {
			handleMasterReq(msg)
		} else if msg.From == SUCCESSOR && msg.Type == ACK_TO_REQ {
			LogStatement.Printf("Received Ack from Successor for request %+v", msg.AckToReq)
			handleSuccessorReq(msg)
		} else if msg.From == PREDECESSOR && msg.Type == PROCESS_REQ {
			// After accepting the processes the request
			LogStatement.Printf("Received seq %d : Received process request from predecessor %+v", ReceivedMsgs, msg.UpdateReq)
			incrementReceiveMsgsCount()
			handlePredecessorReq(msg)
		} else if msg.From == OLD_TAIL && msg.Type == PROCESSED_TRANS && NodeDesignation == "NEW_NODE" {
			LogStatement.Printf("Received Processed Transactions from Tail :: ", msg.TrnsfrProcessedTrans)
			Predecessor = msg.SConfig.TcpAddress
			handleOldTailReq(msg.TrnsfrProcessedTrans)
		} else if msg.From == OTHER_BANK_TAIL {
			handleOtherBankReq(msg)
		}
	}
}

func handleOtherBankReq(msg Message) {
	if msg.SubType == TRANSFER_REQ {
		if MyConfig.TransferReqFailure != true {
			LogStatement.Printf("Received Transfer request from other bank :: %+v \n", msg.UpdateReq)
			update := reFrameUpdateReq(msg.UpdateReq)
			reply := processClientUpdateRequest(update, msg.Client.ClientAddress)
			LogStatement.Printf("Processed other bank request and reply is %+v \n", reply)
			sendReqToSuccessor(msg.Client.ClientAddress, update, Successor)
		} else {
			Abort = true
			LogStatement.Printf("Received Transfer request from other bank :: %+v \n", msg.UpdateReq)
			// LogStatement.Printf("Recieved Transfer req and failing...\n")
		}
	} else if msg.SubType == TRANSFER_ACK {
		reply := msg.RequestReply
		LogStatement.Printf("Received Transfer ack from other bank :: %+v \n", msg.RequestReply)
		sendAckToPredecessor(Req{msg.RequestReply.ReqID}, Predecessor)

		transaction, present := processedTrans[reply.ReqID]
		if present == true {
			reply.AccountNum = transaction.AccountNum
			reply.Balance = userAccounts[transaction.AccountNum].Balance
			reqId := reply.ReqID
			for i := 0; i < len(sentReqIds); i++ {
				if reqId == sentReqIds[i] {
					copy(sentReqIds[i:], sentReqIds[i+1:])
					sentReqIds = sentReqIds[:len(sentReqIds)-1]
					break
				}
			}
			sendReplyToClient(transaction.ClientId, reply)
		}
	}
}

func reFrameUpdateReq(update Update) (newUpdate Update) {
	newUpdate.UpdateType = update.UpdateType
	newUpdate.ReqID = update.ReqID
	newUpdate.AccountNum = update.DestAccountNum
	newUpdate.Amount = update.Amount
	newUpdate.SrcBank = update.SrcBank
	newUpdate.DestBank = update.DestBank
	newUpdate.DestAccountNum = update.AccountNum
	return
}

func handleSuccessorReq(msg Message) {
	reqId := msg.AckToReq.ReqID
	for i := 0; i < len(sentReqIds); i++ {
		if reqId == sentReqIds[i] {
			copy(sentReqIds[i:], sentReqIds[i+1:])
			sentReqIds = sentReqIds[:len(sentReqIds)-1]
			break
		}
	}

	if NodeDesignation != "HEAD" {
		sendAckToPredecessor(msg.AckToReq, Predecessor)
	}
}

func sendAckToPredecessor(sentReq Req, predecessor string) {
	conn, err := net.Dial("tcp", predecessor)
	if err != nil {
		LogStatement.Printf("Dial to Predecessor error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = SUCCESSOR
	msg.Type = ACK_TO_REQ
	msg.Bank = Bank
	msg.AckToReq = sentReq

	message, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("Predecessor ack request Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(message))
	if tcpErr != nil {
		LogStatement.Printf("Predecessor ack request write error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Request Ack sent successfully to predecessor %+v \n", msg.AckToReq)
	// incrementSendMsgsCount()
}

func sendAllProcessedTransToNewTail(newNodeTcpAddress string) {

	if MyConfig.NewNodeTailFailure == true {
		Abort = true
	}

	if MyConfig.NewNodeTailFailure != true {
		conn, err := net.Dial("tcp", newNodeTcpAddress)
		if err != nil {
			LogStatement.Printf("Tail Dial to New node error :  %s \n", err)
		}
		defer conn.Close()

		transferMsg := TransferProcessedTrans{}
		transferMsg.ProcessedTrans = processedTrans
		transferMsg.Accounts = userAccounts
		msg := Message{}
		msg.From = OLD_TAIL
		msg.Type = PROCESSED_TRANS
		msg.TrnsfrProcessedTrans = transferMsg
		msg.SConfig = MyConfig

		message, err := json.Marshal(msg)
		if err != nil {
			LogStatement.Printf("Tail processed trans Json marshall failed : %s \n", err)
		}

		_, tcpErr := conn.Write([]byte(message))
		if tcpErr != nil {
			LogStatement.Printf("Tail processed trans write error : %s \n", tcpErr)
		}

		conn.Close()
		LogStatement.Printf("Tail sent processed trans to new node successfully %+v \n", msg.TrnsfrProcessedTrans)
		// incrementSendMsgsCount()
	} else {
		LogStatement.Printf("Tail doesn't sent processed trans to new node and dead \n")
	}
}

func sendProcessedTransStatusToNewSuccessor(nodeStatus ServerNodeStatus, newSuccessor ServerConfig) {

	// LogStatement.Printf("Updated new Successor TCP Address %+v \n", Successor)
	if MyConfig.SMinusAbort == true {
		Abort = true
	}

	if MyConfig.SMinusAbort != true {
		LogStatement.Printf("Predecessor going to send new processed trans Status to new successor\n")
		LogStatement.Println("Sent Reqs queue :: ", sentReqIds)

		for i := 0; /*nodeStatus.ProcessedTransCount*/ i < len(sentReqIds); i++ {
			transaction, present := processedTrans[sentReqIds[i]]
			if present == true {
				update := Update{transaction.RequestType, transaction.ReqID,
					transaction.AccountNum, transaction.Amount, transaction.DestAccountNum, transaction.DestBank, Bank}
				// msg := Message{}
				// msg.Client.ClientAddress = transaction.ClientId
				// msg.UpdateReq = update
				sendReqToSuccessor(transaction.ClientId, update, newSuccessor.TcpAddress)
			}
		}
		LogStatement.Printf("Predecessor sent new processed trans to new successor successfully \n")
	} else {
		LogStatement.Printf("Predecessor doesn't send processed trans to new successor and dead")
	}
}

func claimAsNewTailToMaster() {
	// LogStatement.Printf("Updated new Successor TCP Address %+v \n", Successor)
	if MyConfig.NewNodeFailure == true {
		Abort = true
	}

	if MyConfig.NewNodeFailure != true {
		conn, err := net.Dial("tcp", MasterMiscTCP)
		if err != nil {
			LogStatement.Printf("Master Misc Dial Connection error :  %s \n", err)
		}
		defer conn.Close()

		msg := Message{}
		msg.From = NEW_TAIL
		msg.Type = CLAIM_ROLE
		msg.NewRoleMsg.Role = "TAIL"
		msg.Bank = Bank
		message, err := json.Marshal(msg)
		if err != nil {
			LogStatement.Printf("TCP Server to Master Misc Json marshall failed : %s \n", err)
		}

		_, tcpErr := conn.Write([]byte(message))
		if tcpErr != nil {
			LogStatement.Printf("TCP Server to Master Misc write error : %s \n", tcpErr)
		}

		conn.Close()
		LogStatement.Printf("Claim msg sent successfully to master : %+v \n", msg.NewRoleMsg)
		// incrementSendMsgsCount()
	} else {
		LogStatement.Printf("Claim msg not sent to master and dead")
	}
}

func sendProcessedTransStatusToMaster(nodeStatus ServerNodeStatus) {
	conn, err := net.Dial("tcp", MasterMiscTCP)
	if err != nil {
		LogStatement.Printf("To send present Transaction status, Master Misc Dial Connection error :  %s \n", err)
	}
	defer conn.Close()

	msg := Message{}
	msg.From = SERVERNODE
	msg.Type = PROCESSED_TRANS_STATUS
	msg.NodeStatus = nodeStatus
	msg.Bank = Bank

	jsonMessage, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Printf("To send present Transaction status, TCP Server to Master Misc Json marshall failed : %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(jsonMessage))
	if tcpErr != nil {
		LogStatement.Printf("To send present Transaction status, TCP Server to Master Misc write error : %s \n", tcpErr)
	}

	conn.Close()
	LogStatement.Printf("Sent processed Transactions Status to Master : %+v \n", msg.NodeStatus)
	if MyConfig.SPlusAbort == true {
		Abort = true
		// LogStatement.Println("HELP HELP HELP")
	}
	// incrementSendMsgsCount()
}

// Start up delay
func startServerDelay(delay int) {
	ticker := time.NewTicker(time.Duration(delay) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				LogStatement.Printf("Started the server...\n")
				startServer()
				ticker.Stop()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func startServer() {
	go listenTCP()

	if NodeDesignation == "NEW_NODE" {
		sendJoinReqToMaster()
	}

	go sendPingToMaster(MyConfig.PingDelay)
	if NodeDesignation == "HEAD" {
		go listenUpdateUDPConn()
	} else if NodeDesignation == "TAIL" {
		go listenQueryUDPConn()
	}
}

/*func LogMessage(msgType string, msg interface{}){
  temp := 0
  fileName := "Server_" + ServerId + ".log"
  f, fileOpenError := os.OpenFile("../../logs/" + fileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
  if fileOpenError != nil {
    log.Fatalf("error opening file: %v", fileOpenError)
  }

  if msgType == "Received" {
    receiveSeq++
    temp=receiveSeq
  } else if msgType == "Send" {
    sendSeq++
    temp=sendSeq
  }

  defer f.Close()

	log.SetOutput(f)
  if temp != 0 {
    log.Println(msgType," Sequence Number:",temp," ",msg)
  } else {
    log.Println(msg)
  }
}*/
func sendTCPMessage(destination string, msg interface{}, log interface{}) {
	conn, err := net.Dial("tcp", destination)
	if err != nil {
		LogStatement.Println(log, "dial error:	\n", err)
	}
	defer conn.Close()

	jsonMessage, err := json.Marshal(msg)
	if err != nil {
		LogStatement.Println(log, "json marshall error:  %s \n", err)
	}

	_, tcpErr := conn.Write([]byte(jsonMessage))
	if tcpErr != nil {
		LogStatement.Println(log, "write error:  %s \n", tcpErr)
	}

	conn.Close()
	// LogStatement.Println(log, "sent successfully")
	// incrementSendMsgsCount()
}

func incrementSendMsgsCount() {
	SentMsgs++
}

func incrementReceiveMsgsCount() {
	ReceivedMsgs++
}
