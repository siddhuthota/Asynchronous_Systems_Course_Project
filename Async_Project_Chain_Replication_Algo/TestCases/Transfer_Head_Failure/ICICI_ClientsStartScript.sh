#!/bin/bash
# BankName FileIndexOfConfig ClientIndex RequestType(Deterministic(0)/Probablistic(1))
go run ../Code/client.go ICICI 0 0 2 &
#go run ../Code/client.go 1 0 0 &
#go run ../Code/client.go 2 0 0 &
#go run ../Code/client.go 3 0 0 &
wait
echo "All the Clients are closed"  

