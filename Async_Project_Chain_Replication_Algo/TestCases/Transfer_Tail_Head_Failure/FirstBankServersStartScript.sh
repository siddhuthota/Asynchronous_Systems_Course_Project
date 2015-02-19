#!/bin/bash
# BankName FileIndexOfConfig ServerIndex
go run ../Code/Head.go ICICI 3 0 &
go run ../Code/Head.go ICICI 3 1 & 
go run ../Code/Head.go ICICI 3 2 &
wait
echo "All the servers are working..."  
