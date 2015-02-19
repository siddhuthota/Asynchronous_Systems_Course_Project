#!/bin/bash
# BankName FileIndexOfConfig ServerIndex
go run ../Code/Head.go HDFC 0 0 &
go run ../Code/Head.go HDFC 0 1 & 
go run ../Code/Head.go HDFC 0 2 &
wait
echo "All the servers are working..."  
