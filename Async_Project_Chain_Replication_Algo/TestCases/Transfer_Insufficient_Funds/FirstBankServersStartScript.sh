#!/bin/bash
# BankName FileIndexOfConfig ServerIndex
go run ../Code/Head.go ICICI 5 0 &
go run ../Code/Head.go ICICI 5 1 & 
go run ../Code/Head.go ICICI 5 2 &
wait
echo "All the servers are working..."  
