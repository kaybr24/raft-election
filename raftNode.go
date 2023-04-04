package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type RaftNode int

// type Timer struct {
// 	C <-chan time.Time
// 	// contains filtered or unexported fields
// }

// // keep track of which server votes for who and in which term
// type ElectionResults struct {
// 	CandidateID int //candidate requesting vote
// 	totalVotesFor int //number of votes for that candidate
// }

// input for RequestVote RPC
// not using lastLogIndex; lastLogTerm
type VoteArguments struct {
	Term        int // candidate's term
	CandidateID int // candidate requesting vote
}

// output for RequestVote RPC
type VoteReply struct {
	Term       int  // current term, for candidate to update itself
	ResultVote bool // true if candidate recieved vote
}

// input for AppendEntries RPC
// prevLogIndex, prevLogTerm, entries[],and leaderCommit all refer to logs and will not be used here
type AppendEntryArgument struct {
	Term     int // leader's term
	LeaderID int // so follower can redirect clients
}

// output for AppendEntries RPC
type AppendEntryReply struct {
	Term    int  // current term, allowing leader to update itself
	Success bool // true if term >= currentTerm; normally: true if follower contained entry matching prevLogIndex and prevLogTerm
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

var selfID int
var serverNodes []ServerConnection
var currentTerm int
var votedFor int
var electionTimeout *time.Timer
var isLeader bool

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	if isLeader {
		isLeader = false
		fmt.Println("Was leader, NOW follower")
	}
	fmt.Printf("Candidate %d is requesting a vote from Follower %d\n", arguments.CandidateID, selfID)
	// if candidate's term is less the global currentTerm than reply.ResultVote = FALSE
	if arguments.Term < currentTerm {
		reply.ResultVote = false
		reply.Term = currentTerm // let the candidate know what the term is
		fmt.Printf("REJECTED: Candidate %d's term #%d is less than current term #%d\n", arguments.CandidateID, arguments.Term, currentTerm)
	} else if votedFor != 0 && votedFor != arguments.CandidateID { // candidate is valid, but server already voted for someone else
		reply.ResultVote = false
		fmt.Printf("REJECTED Candidate %d because Follower %d has already voted for %d\n", arguments.CandidateID, selfID, votedFor)
	} else { //vote for the candidate
		reply.ResultVote = true
		fmt.Printf("VOTED FOR Candidate %d\n", arguments.CandidateID)
	}
	return nil

}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	fmt.Printf("Got RPC from Leader %d in term #%d\n", arguments.LeaderID, arguments.Term)
	//stop timer
	timer_was_going := electionTimeout.Stop()
	if timer_was_going {
		fmt.Println("Heartbeat recieved; time was stopped\n")
	}
	//restart timer
	StartTimer()
	//if the candidate's term is less than global currentTerm, reply.Success = FALSE
	return nil
}

// generate random duration
func randomTime() time.Duration {
	//rand.Seed(time.Now().UnixNano())
	min := 150  //ms
	max := 1000 //ms
	return time.Duration(rand.Intn(max-min+1) + min)
}

func StartTimer() {
	//start as a follower
	fmt.Printf("Follower %d is HERE", selfID)
	electionTimeout = time.NewTimer(randomTime() * time.Millisecond)
	go func() {
		<-electionTimeout.C //.C is a channel time object
		//become a candidate by starting leader election
		fmt.Printf("Election Timeout: %d is initating elections\n", selfID)
		LeaderElection()
	}()
	// if we get an RPC, restart the timer
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	currentTerm += 1 // increment current term
	voteCounts := 1  //vote for self
	fmt.Printf(">> Recieved VOTE: self -> %d\n", selfID)
	//reset election timer (didn't we just do this?)
	//send RequestVote RPC to all other servers
	voteArgs := new(VoteArguments)
	voteArgs.Term = currentTerm
	voteArgs.CandidateID = selfID
	var voteResult *VoteReply
	voteResult = new(VoteReply)
	for _, node := range serverNodes {
		serverCall := node.rpcConnection.Go("RaftNode.RequestVote", voteArgs, voteResult, nil)
		<-serverCall.Done
		if voteResult.ResultVote {
			voteCounts += 1 //recieved a vote
			fmt.Printf(">> Recieved VOTE: %d -> %d\n", node.serverID, selfID)
		} else {
			fmt.Printf(">> %d REJECTED %d \n", node.serverID, selfID)
		}
	}
	//if recieved majority of votes, become leader
	if voteCounts >= len(serverNodes)/2 {
		fmt.Printf("Elected LEADER %d with %d/%d of the vote", selfID, voteCounts, len(serverNodes))
		isLeader = true
		Heartbeat() //Make this continue until RequestVote is recieved
	}
	// if AppendEntries RPC recieved from new leader, convert to follower

	// if election timeout elapses, start a new election
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	for isLeader {
		fmt.Printf("heartbeat - leader %d is pinging all servers!\n", selfID)
		arg := new(AppendEntryArgument)
		reply := new(AppendEntryReply)
		for _, node := range serverNodes {
			node.rpcConnection.Go("RaftNode.AppendEntry", arg, &reply, nil)
		}
		time.Sleep(10 * time.Second) //pause
	}
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	selfID = myID //define raftNode's ID
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	for index, element := range lines {
		// Attempt to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft
	fmt.Printf("Creating Follower %d\n", selfID)
	StartTimer()
	isLeader = false
	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

}
