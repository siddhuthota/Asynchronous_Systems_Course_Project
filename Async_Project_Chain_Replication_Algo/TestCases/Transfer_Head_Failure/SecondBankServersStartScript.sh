#!/bin/bash
# BankName FileIndexOfConfig ServerIndex
go run ../Code/Head.go HDFC 2 0 &
go run ../Code/Head.go HDFC 2 1 & 
go run ../Code/Head.go HDFC 2 2 &
wait
echo "All the servers are working..."  
