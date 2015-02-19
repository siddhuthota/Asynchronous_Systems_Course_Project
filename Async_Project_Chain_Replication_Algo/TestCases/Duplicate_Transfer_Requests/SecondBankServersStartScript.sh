#!/bin/bash
# BankName FileIndexOfConfig ServerIndex
go run ../Code/Head.go HDFC 4 0 &
go run ../Code/Head.go HDFC 4 1 & 
go run ../Code/Head.go HDFC 4 2 &
wait
echo "All the servers are working..."  
