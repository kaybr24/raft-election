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
var timerDuration time.Duration

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	//all RPCs function as heartbeats
	//restartTimer2()
	//this should not print
	fmt.Printf("Candidate %d is requesting a vote from Follower %d\n", arguments.CandidateID, selfID)
	//clear the votedFor if we are in a new election
	if arguments.Term > currentTerm {
		votedFor = -1
	}

	//if this machine hasn't voted yet in this term, voted for will be -1
	if votedFor == -1 {
		//if the request is coming from a lesser term, then don't vote for it
		if arguments.Term < currentTerm {
			reply.ResultVote = false
			//tell the candidate our term?
			reply.Term = currentTerm
			fmt.Printf("Server %d (term #%d) REJECTED Candidate %d (term #%d) because we are in higher term\n", selfID, currentTerm, arguments.CandidateID, arguments.Term)
		} else { //otherwise if the candidate is higher term (or same term?) as us, we can vote for it
			fmt.Println("---->I GOT TO WHERE I AM GOING TO VOTE")
			reply.ResultVote = true
			currentTerm = arguments.Term //update term
			if isLeader {
				isLeader = false //no longer leader (if previously leader)
				fmt.Printf("I was leader, but now I am not\n")
			}
			votedFor = arguments.CandidateID
			fmt.Printf("VOTED FOR Candidate %d in term #%d\n", arguments.CandidateID, currentTerm)
		} /*else {
			fmt.Println("--->FIGURE OUT WHAT TO DO IN THIS SCENARIO")
		}*/
	} else if votedFor == arguments.CandidateID {
		fmt.Println("---->I GET TO WHERE I AM GOING TO VOTE")
		reply.ResultVote = true
		currentTerm = arguments.Term //update term
		if isLeader {
			isLeader = false //no longer leader (if previously leader)
			fmt.Printf("I was leader, but now I am not\n")
		}
		votedFor = arguments.CandidateID
		fmt.Printf("VOTED FOR Candidate %d in term #%d\n", arguments.CandidateID, currentTerm)
	} else {
		fmt.Printf("I already voted for %d in this term #%d\n", votedFor, currentTerm)
		reply.ResultVote = false
		reply.Term = currentTerm
	}

	/*
		//clear the votedFor if we are in a new election
		if arguments.Term > currentTerm {
			votedFor = -1
		}
		// // Now do we want to vote for them?
		// if votedFor != -1 { //we already voted for someone in this term
		// 	reply.ResultVote = false
		// 	fmt.Printf("Server %d (term #%d) REJECTED Candidate %d (term #%d) - voted for %d\n", selfID, currentTerm, arguments.CandidateID, arguments.Term, votedFor)
		// } else { //we have not voted for anyone in this term

		// }
		// if isLeader && arguments.Term > currentTerm {
		// 	isLeader = false
		// 	fmt.Println(">> Was leader, NOW follower")
		// }

		// if candidate's term is less the global currentTerm than reply.ResultVote = FALSE
		if arguments.Term >= currentTerm && votedFor == -1 { //A leader already voted for itself
			reply.ResultVote = true
			currentTerm = arguments.Term //update term
			isLeader = false             //no longer leader (if previously leader)
			votedFor = arguments.CandidateID
			fmt.Printf("VOTED FOR Candidate %d in term #%d\n", arguments.CandidateID, currentTerm)
		} else {
			reply.ResultVote = false
			reply.Term = currentTerm
			fmt.Printf("Server %d (term #%d) REJECTED Candidate %d (term #%d) - voted for %d\n", selfID, currentTerm, arguments.CandidateID, arguments.Term, votedFor)
		}
		// if arguments.Term < currentTerm {
		// 	reply.ResultVote = false
		// 	reply.Term = currentTerm // let the candidate know what the term is
		// 	fmt.Printf("REJECTED: Candidate %d's term #%d is less than current term #%d\n", arguments.CandidateID, arguments.Term, currentTerm)
		// } else if votedFor == selfID { // candidate is valid, but we are also a candidate
		// 	reply.ResultVote = false
		// 	fmt.Printf("REJECTED Candidate %d because WE are a Candidate (%d)\n", arguments.CandidateID, selfID)
		// } else if votedFor != arguments.CandidateID { // candidate is valid, but server already voted for someone else
		// 	reply.ResultVote = false
		// 	fmt.Printf("REJECTED Candidate %d because Follower %d has already voted for %d\n", arguments.CandidateID, selfID, votedFor)
		// } else { //vote for the candidate
		// 	reply.ResultVote = true
		// 	fmt.Printf("VOTED FOR Candidate %d\n", arguments.CandidateID)
		// }*/
	return nil

}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	fmt.Printf("Got RPC from Leader %d in term #%d\n", arguments.LeaderID, arguments.Term)
	stopped := restartTimer()
	//if recieves a heartbeat, we must be a follower
	if isLeader {
		isLeader = false //no longer leader (if previously leader)
		fmt.Printf("I was leader, but now I am not (found out when receiving a heartbeat)")
	}

	//might need to update our term
	if arguments.Term != currentTerm {
		fmt.Printf("-- heartbeat CHANGED term from %d to %d", currentTerm, arguments.Term)
		currentTerm = arguments.Term
	}

	if !stopped {
		fmt.Printf("leader %d's heartbeat recieved AFTER server %d's timer stopped'\n", arguments.LeaderID, selfID)
	}
	//if the candidate's term is less than global currentTerm, reply.Success = FALSE
	return nil
}

// generate random duration
func randomTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	min := 1500  //ms
	max := 10000 //ms
	t := rand.Intn(max-min+1) + min
	//fmt.Printf("Server %d is using a %d ms timer\n", selfID, t)
	return time.Duration(t)
}

func StartTimer(t time.Duration) {
	//start as a follower
	//fmt.Printf("Follfower %d is HERE\n", selfID)
	electionTimeout = time.NewTimer(t * time.Millisecond)
	go func() {
		<-electionTimeout.C //.C is a channel time object
		//become a candidate by starting leader election
		fmt.Printf("\nElection Timeout: %d is initating elections in term #%d\n", selfID, currentTerm+1)
		LeaderElection()
	}()
	// if we get an RPC, restart the timer
}

func restartTimer() bool {
	//stop timer
	timer_was_going := electionTimeout.Stop()
	if timer_was_going {
		fmt.Println("Heartbeat recieved; timer was stopped")
	}
	//restart timer
	StartTimer(timerDuration)
	return timer_was_going
}

// func restartTimer2() bool {
// 	//stop timer
// 	timer_was_going := electionTimeout.Stop()
// 	if timer_was_going {
// 		fmt.Println("Heartbeat recieved; timer was stopped")
// 	}
// 	//restart timer
// 	//StartTimer()
// 	electionTimeout.Reset(timerDuration * time.Millisecond)
// 	return timer_was_going
// }

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	currentTerm += 1 // increment current term
	//vote for self
	votedFor = selfID
	voteCounts := 1
	fmt.Printf(">> Recieved VOTE: self -> %d\n", selfID)
	//reset election timer (didn't we just do this?)
	//send RequestVote RPC to all other servers
	voteArgs := new(VoteArguments)
	voteArgs.Term = currentTerm
	voteArgs.CandidateID = selfID
	//var voteResult *VoteReply //NEED TO CREATE A NEW VOTEREPLY OBJECT FOR EACH CALL (I THINK PUT INSIDE THE FOR LOOP)
	//voteResult = new(VoteReply)
	for _, node := range serverNodes {
		fmt.Printf("requesting vote from: ")
		fmt.Println(node)
		//var voteResult *VoteReply //NEED TO CREATE A NEW VOTEREPLY OBJECT FOR EACH CALL (I THINK PUT INSIDE THE FOR LOOP)
		voteResult := new(VoteReply)

		serverCall := node.rpcConnection.Go("RaftNode.RequestVote", voteArgs, voteResult, nil)
		<-serverCall.Done
		if voteResult.ResultVote {
			voteCounts += 1 //recieved a vote
			fmt.Printf(">> Recieved VOTE: %d -> %d\n", node.serverID, selfID)
		} else {
			fmt.Printf(">> %d REJECTED %d \n", node.serverID, selfID)
			currentTerm = voteResult.Term //catch up to current term
		}
	}
	//if recieved majority of votes, become leader
	voteProportion := float64(voteCounts) / float64(len(serverNodes))
	if voteProportion >= 0.5 {
		fmt.Printf("Elected LEADER %d with %d out of %d of the vote in TERM #%d\n", selfID, voteCounts, len(serverNodes)+1, currentTerm)
		electionTimeout.Stop()
		isLeader = true
		Heartbeat() //Make this continue until RequestVote is recieved
	}
	// if AppendEntries RPC recieved from new leader, convert to follower

	// if election timeout elapses, start a new election
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	arg := new(AppendEntryArgument)
	arg.Term = currentTerm
	arg.LeaderID = selfID
	for isLeader {
		fmt.Printf("heartbeat - leader %d is pinging all servers!\n", selfID)
		reply := new(AppendEntryReply)
		for _, node := range serverNodes {
			node.rpcConnection.Go("RaftNode.AppendEntry", arg, &reply, nil)
		}
		time.Sleep(1 * time.Second) //pause
		//if you want to introduce failures, randomly break in that loop
		if rand.Intn(10) > 5 {
			break //pretend to fail
		}
		//alternatively, could set up one of the machines to have a really short timeout and all the others have a really long timeout to mimic a failure
	}
}

func main() {
	//var wg sync.WaitGroup
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
	if err != nil {
		log.Fatal(err)
	}
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
	offset := 0
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
		if index == myID {
			offset = 1
		}
		serverNodes = append(serverNodes, ServerConnection{index + offset, element, client})
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft
	fmt.Printf("Creating Follower %d\n", selfID)
	timerDuration = randomTime()
	//wg.Add(1)
	isLeader = false
	StartTimer(timerDuration) //go
	//wg.Wait()
	time.Sleep(50 * time.Second)
	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

}
