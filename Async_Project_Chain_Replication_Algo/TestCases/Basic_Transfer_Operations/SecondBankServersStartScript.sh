#!/bin/bash
# BankName FileIndexOfConfig ServerIndex
go run ../Code/Head.go HDFC 1 0 &
go run ../Code/Head.go HDFC 1 1 & 
go run ../Code/Head.go HDFC 1 2 &
wait
echo "All the servers are working..."  
