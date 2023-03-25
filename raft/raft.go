package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log ent ry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"cs350/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "cs350/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// -------------------------------------------------------   CODE  ----------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------------------------------------------
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//states constant across all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogItem

	//states volatile for all servers
	CommitIndex int
	LastApplied int

	//states needed by the leader
	NextIndex  []int
	MatchIndex []int

	// server can be follower, candidate or leader
	ServerState string
	// recieving a rpc from leader or candidate
	CheckForLeader bool

	// store time as a timestamp for elections and heartbeats
	HeartbeatRecieved time.Time
	ElectionTimer     time.Time

	lastHeartBeat bool

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// -------------------------------------------------------------------------------------------------
type LogItem struct {
	Term    int         // store the term
	Command interface{} // since raft is agnostic to the commands it recieved, command can be of anytype and thus interface
}

// -------------------------------------------------------------------------------------------------
func (rf *Raft) getLastLogItem() (int, int) {
	rf.mu.Lock()
	lastLogIndex := len(rf.Log) - 1
	lastLogTerm := rf.Log[lastLogIndex].Term
	rf.mu.Unlock()
	return lastLogIndex, lastLogTerm
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool
	//code

	term = rf.CurrentTerm
	if rf.ServerState == "leader" {
		isLeader = true
	} else {
		isLeader = false
	}

	return term, isLeader
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// APPENDENTRIES STRUCT AND FUNCTIONS
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogItem
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		fmt.Println("AppendEntriesRPC: Server killed")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		//live example, rf = 8, args = 6
		fmt.Println("AppendEntries: Leader is lagging behind server, convert to a follower")
		return
	}

	if args.Term > rf.CurrentTerm {
		//live example, rf = 6, args = 8
		fmt.Println("AppendEntries: Follower is behind leader, updated terms!")
		rf.CurrentTerm = args.Term
		rf.ServerState = "follower"
		rf.VotedFor = -1
	}

	rf.lastHeartBeat = true
	reply.Term = rf.CurrentTerm
	reply.Success = true
}

// -------------------------------------------------------------------------------------------------
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// Call() sends a request and waits, if the reply comes with a timeout, its true otherwise Call() returns false
	return ok
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		fmt.Println("RequestVoteRPC: Server killed")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println("starting the Request Vote rpc call")
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.ServerState = "follower"
		rf.VotedFor = -1
		fmt.Println("Request Vote: current server is behind leader. Server voting has a Term - ", rf.CurrentTerm)
	}

	if args.Term < rf.CurrentTerm {
		fmt.Println("Request Vote: Leader lags behind server")
		rf.VotedFor = -1
		return
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		fmt.Println("Server: ", rf.me, " voted for: ", args.CandidateId, "Voting server's term is: ", rf.CurrentTerm)
	}
}

// -------------------------------------------------------------------------------------------------

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// -------------------------------------------------------------------------------------------------
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// ---------------------------------------------------------------------------------------------------------------------------------------------------
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
// ---------------------------------------------------------------------------------------------------------------------------------------------------
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// START NEW ELECTION AND TICKER
func (rf *Raft) startNewElection() {

	rf.mu.Lock()
	rf.CurrentTerm++
	rf.ServerState = "candidate"
	term := rf.CurrentTerm
	id := rf.me
	rf.VotedFor = id
	peers := rf.peers
	rf.mu.Unlock()

	var votesRecieved int32 = 0

	for server := range peers {
		if server == id {
			atomic.AddInt32(&votesRecieved, 1)
			continue
		}

		args := RequestVoteArgs{
			Term:        term,
			CandidateId: id,
		}
		reply := RequestVoteReply{}

		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()

			if !ok {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.ServerState = "follower"
				rf.VotedFor = -1
				rf.CurrentTerm = reply.Term
				// votesRecieved = 0
				atomic.StoreInt32(&votesRecieved, 0)
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&votesRecieved, 1)
				// fmt.Println("-----------------------------------------------")
				if atomic.LoadInt32(&votesRecieved) > int32(len(peers)/2) {
					rf.ServerState = "leader"
					votesRecieved = 0
					fmt.Println("NEW LEADER ELECTED: ", rf.me)
					go rf.sendHeartbeats()
				}
			}
			rf.mu.Unlock()
		}(server, &args, &reply)
	}
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// SEND HEARTBEATS
func (rf *Raft) sendHeartbeats() {
	//	---	---	---	--- --- --- --- --- --- --- --- Run till the server is killed or server is not leader --- --- --- --- --- ------ ---
	// The newly elected leader sends heartbeats through the AppendEntries Rpc which is empty
	for !rf.killed() {

		rf.mu.Lock()
		serverState := rf.ServerState
		term := rf.CurrentTerm
		id := rf.me
		peers := rf.peers
		rf.mu.Unlock()

		if serverState == "leader" {
			//send heartbeats every 100ms
			fmt.Println("Server - ", id, " is starting Heartbeats!")
			//	---	---	---	--- --- --- --- --- --- --- --- Loop through servers to send Empty AppendEntries --- --- --- --- --- ------ ---
			for server := range peers {

				if server == id {
					continue
				}
				//should be empty
				args := &AppendEntriesArgs{
					Term:     term,
					LeaderID: id,
				}
				reply := &AppendEntriesReply{}

				//	---	---	---	--- --- --- --- --- --- --- --- Send Go routines to followers --- --- --- --- --- ------ ---
				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					ok := rf.sendAppendEntries(server, args, reply)
					rf.mu.Lock()
					// defer rf.mu.Unlock()
					//Either send successful empty appendEntries or fail in sending the rpc call
					if !ok || rf.ServerState != "leader" {
						// fmt.Println("sendHeartbeats: Returning!")
						rf.mu.Unlock()
						return
					}

					if reply.Term > rf.CurrentTerm {
						fmt.Println("sendHeartbeats: Converting to a follower")
						rf.ServerState = "follower"
						rf.VotedFor = -1
						rf.CurrentTerm = reply.Term
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						// fmt.Println("sendHeartbeats: Success in sending empty rpc!")
						fmt.Println(" ---------------------------------- ")
					} else {
						fmt.Println("sendHeartbeats: Reply is unsuccesssful!")
					}
					rf.mu.Unlock()
				}(server, args, reply)
			}
			time.Sleep(120 * time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// Helper function to set random election timeouts
func setRandomElectionTimer() time.Duration {
	definedTimeValue, randomTime := 850, 950
	randTime := definedTimeValue + rand.Intn(randomTime)
	duration := time.Duration(randTime) * time.Millisecond
	return duration
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		electionTimeout := setRandomElectionTimer()
		time.Sleep(electionTimeout)
		fmt.Println("Election timeout, sleeping!")
		rf.mu.Lock()
		if rf.lastHeartBeat || rf.ServerState == "leader" {
			rf.lastHeartBeat = false
			fmt.Println("Ticker: Recieved a hearbeat")
			rf.mu.Unlock()
			continue
		} else {
			fmt.Println("Ticker: Staring a new Election")
			rf.mu.Unlock()
			rf.startNewElection()
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// the service or tester wants to create a Raft server. the ports of all the Raft servers (including this one) are in peers[]. this  server's port is peers[me].
// all the servers' peers[] arrays have the same order. persister is a place for this server to save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages. Make() must return quickly, so
// it should start goroutines for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,

		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         make([]LogItem, 0),

		CommitIndex: 0,
		LastApplied: 0,

		NextIndex:  make([]int, 0),
		MatchIndex: make([]int, 0),

		ServerState:    "follower",
		CheckForLeader: false, // no leader in the beginning

		HeartbeatRecieved: time.Now(),
		lastHeartBeat:     false,
	}

	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
}

// ------------------------------------------------------------------   END  -------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------------------------------------------
