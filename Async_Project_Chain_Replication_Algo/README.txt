README.TXT
-----------
We have done our project in Google's GO and Distalgo. This document explains about "GO" part.

INSTRUCTIONS
------------
1. Installation of "GO" from web (In Ubuntu)

	a. Open the terminal and install go using "sudo apt-get install golang" command. If it doesn't work update your repository and try 		again by using below mentioned commands
	sudo add-apt-repository ppa:gophers/go
   	sudo apt-get update
   	sudo apt-get install golang-stable	

	For your reference I also included the webpage I referred to install (https://code.google.com/p/go-wiki/wiki/Ubuntu)
	Hardware requirements are mentioned in this page (https://golang.org/doc/install)

	b. To test the installation has been correct or not check with following:
	i.   Navigate to a directory using terminal
	ii.  Create a file "Hello.go"
	iii. Paste the below code in the document
	     package main
	     import "fmt"

	     func main() {
    		fmt.Printf("hello, world\n")
	     }
	
	iv.  Now save and run the file using the command "go run Hello.go"
	v.   If you see output "hello, world". Then everything went fine. If not please check with your installation 

	c. For your reference, You can also install in Windows but might not be as clean as Ubuntu environment

2. MAIN FILES

	a. FOLDER/Code/Master.go: This contains master code
	b. FOLDER/Code/Head.go: This contains server code
	c. FOLDER/Code/Client.go: This contains client code
	d. FOLDER/Config: Contains all configuration files
	e. FOLDER/logs: contains all generated log files
	f. FOLDER/testing.txt: Contains all the information about testcases we executed 

	All this files are same to all banks servers and all clients. The implementation will only differ with the arguments we give while running specific file. In particular, We give different arguments while running the file to make sure it runs in different mode for different bank.

 
3. BUGS AND LIMITATIONS
	
	As of now, We have not identified any bugs. All the testcases specified in the document are working as expected

4. LANGUAGE COMPARISON

	a. GO
		PROS
		i.   High level language which is not much specific about data types. Datatypes are defined at runtime.
		ii.  GO Had "Channels" & "Waitgroup" concepts to achieve syncronization very easily
		iii. Compiling and Running is easy when compared to other languages
		iv.  As effective as C and C++. Inherited many features from them
		v.   Can run each function as GO routine (thread), with minimal effort. (Having "go" keyword while calling the function call this function and executes on other thread)
 		vi.  Has advance features like Garbage collection, Fastness (Compiles to machine code), Efficiency (Reliable) etc.
		v.   Interfaces act as template here
		vi.  Lines of code (1000+ for Server, 700 for Client, 700 for Master) - Writing a functionality n GO is lengthier

		CONS
		i.   Still known to be experimental language
		ii.  No concept of generics as of now. Maps and strings are only advanced datastructures as of now
		iii. Doesn't have effective IDE as of now. But "Lite IDE" is one which works for basic needs

	b. DistAlgo

		PROS
		i.   High level language which is not much specific about data types. Datatypes are defined at runtime.
		ii.  Lines of code is less when compared to GO
		iii. As effective as Phython. Inherited many features from it
		iv.  Threads is same as Phython and should be written explicitly 

		CONS
		i.   Still known to be experimental language
		ii.  No IP or port can be specified explicitly
		iii. No IDE support we found for DistAlgo
