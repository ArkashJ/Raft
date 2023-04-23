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
	"bytes"
	"cs350/labgob"
	"cs350/labrpc"
	"fmt"
	"log"
	"math"
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
	ElectionTimer time.Time

	lastHeartBeat   bool
	applyCh         chan ApplyMsg
	electionTracker chan struct{}
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
func (rf *Raft) getLastLogIndex() int {
	length := len(rf.Log) - 1
	return length
}

// -------------------------------------------------------------------------------------------------
func (rf *Raft) getLastLogTerm() int {
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.Log[lastLogIndex].Term
	return lastLogTerm
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool

	term = rf.CurrentTerm
	if rf.ServerState == "leader" {
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// function to apply the logs to the state machine by checking how far behind the current applied index is from the index of the latest committed log
func (rf *Raft) ApplyLog() {
	if rf.killed() {
		fmt.Println("ApplyLog: Server is killed")
		return
	}

	rf.mu.Lock()
	i := rf.LastApplied + 1
	for i <= rf.CommitIndex {
		// fmt.Printf("Applying log %d - Server %d\n", i, rf.me)
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.Log[i].Command, CommandIndex: i}
		rf.LastApplied = i
		i++
	}
	rf.mu.Unlock()
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// the service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log. if this server isn't the leader, returns false.
// otherwise start the agreement and return immediately. there is no guarantee that this command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed, this function should return gracefully.
// The first return value is the index that the command will appear at if it's ever committed. the second return value is the current term. the third return
// value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		fmt.Println("Start: Server is killed")
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term, isLeader := -1, rf.CurrentTerm, rf.ServerState == "leader"

	if isLeader {
		index = rf.getLastLogIndex() + 1
		logToBeAdded := LogItem{
			Term:    term,
			Command: command,
		}
		rf.Log = append(rf.Log, logToBeAdded)

		// update matchIndex and nextIndex. My own matchIndex is the new index and for each folllower, nextIndex is the new index + 1
		rf.MatchIndex[rf.me] = index
		for i := range rf.NextIndex {
			rf.NextIndex[i] = index + 1
		}
		rf.persist()
	}
	// fmt.Println("Start: Returning index: " + strconv.Itoa(index) + " term: " + strconv.Itoa(term) + " isLeader: " + strconv.FormatBool(isLeader))
	return index, term, isLeader
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		fmt.Println("AppendEntriesRPC: Server killed")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term, reply.Success = rf.CurrentTerm, false
	lastIndex := rf.getLastLogIndex()
	// lastTerm := rf.getLastLogTerm()
	reply.ConflictIndex, reply.ConflictTerm = -1, -1
	log := rf.Log
	// ------------------------------------- TERM CHECKS ------------------------------------------------
	// Example, rf = 6, args = 8
	// Example, rf = 8, args = 6
	if args.Term < rf.CurrentTerm {
		// fmt.Println("AppendEntries: Leader is lagging behind server, convert to a follower")
		// rf.persist()
		return
	}

	rf.lastHeartBeat = true
	if args.Term > rf.CurrentTerm {
		// fmt.Println("AppendEntries: Follower is behind leader, updated terms!")
		rf.CurrentTerm = args.Term
		rf.ServerState = "follower"
		rf.VotedFor = -1
	}

	reply.Term = rf.CurrentTerm
	reply.Success = true

	// ------------------------------------ LOG Consistency Property ------------------------------------
	// Follower's log is smaller than leaders log
	if args.PrevLogIndex > lastIndex {
		// fmt.Println("AppendEntries: Follower's log: ", lastIndex, " is smaller than leaders log: ", args.PrevLogIndex)
		reply.ConflictIndex = lastIndex + 1
		reply.Success = false
		return
	}

	if args.PrevLogIndex >= 0 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// fmt.Println("AppendEntries: Log consistency check failed, terms at the same index do NOT MATCH")
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		i--
		for ; i >= 0 && rf.Log[i].Term == rf.getLastLogTerm(); i-- {
			reply.ConflictIndex = i
			// fmt.Println("The conflict is at Index, " + strconv.Itoa(reply.ConflictIndex) + " and Term, " + strconv.Itoa(lastTerm))
		}
		reply.ConflictIndex++ // new line added
		reply.Success = false
		return
	}

	//----------------------------------------- LOG REPLICATION -----------------------------------------
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	i := args.PrevLogIndex + 1
	j := 0
	for i < lastIndex+1 && j < len(args.Entries) {
		if log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}
	// Append any new entries not already in the log
	// fmt.Println("Clearing the log from index: ", i)
	rf.Log = rf.Log[:i]
	rf.Log = append(rf.Log, args.Entries[j:]...)

	// ----- Setting reply to success -------
	rf.lastHeartBeat = true
	reply.Term = rf.CurrentTerm
	reply.Success = true
	// ----- LEADER COMMIT ------------------
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastIndex)))
		fmt.Println("AppendEntries: Leader commit index is greater than follower's commit index, updating commit index to: ", rf.CommitIndex)
		// fmt.Println("AppendEntries: Applying log to state machine")
		go rf.ApplyLog()
	}
	// rf.persist()
}

// --- Finding CurrentServer ---
func (rf *Raft) isCurrentSever(server int) bool {
	return server == rf.me
}

// --- Checking Leader and Terms ---
func (rf *Raft) handleTerm(reply *AppendEntriesReply) bool {
	if rf.ServerState != "leader" {
		return true
	}

	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.ServerState = "follower"
		rf.VotedFor = -1
		rf.persist()
		return true
	}
	return false
}

// --- Updating MatchIndex For Success ---
func (rf *Raft) handleSuccessfulReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	lenArgsEntries := len(args.Entries)
	// update match index to make the appended entries match
	if rf.MatchIndex[server] < args.PrevLogIndex+lenArgsEntries {
		rf.MatchIndex[server] = args.PrevLogIndex + lenArgsEntries
	}
	rf.NextIndex[server] = rf.MatchIndex[server] + 1
	return false
}

// --- Update Commit Index ---
func (rf *Raft) updateCommitIndex() {
	lastIndex, lenPeers, commitIndex := rf.getLastLogIndex(), len(rf.peers), rf.CommitIndex
	for N := lastIndex; N >= commitIndex; {
		sameNumLogs := 0
		sameNumLogs = sameNumLogs + 1
		// if the term of the log entry is same as the current term, then it is a valid entry
		if rf.Log[N].Term == rf.CurrentTerm {
			server := 0
			for ; server < lenPeers; server++ {
				if rf.MatchIndex[server] >= N && !rf.isCurrentSever(server) {
					// fmt.Println("if the server is not, and the match index is greater than the last index, the number of logs to be replicated is incremented")
					sameNumLogs++
				}
			}
		}
		// if the majority of the servers have the same log entry, then update the commit index
		if sameNumLogs > lenPeers/2 {
			// fmt.Printf("Updating CommitIndex to %d - Server %d\n", N, rf.me)
			rf.CommitIndex = N
			go rf.ApplyLog()
			rf.persist()
			break
		} else {
			N--
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- Reply Fails ---
func (rf *Raft) handleFailedReply(server int, reply *AppendEntriesReply) {
	if reply.ConflictIndex >= 0 {
		rf.NextIndex[server] = reply.ConflictIndex
		rf.MatchIndex[server] = max(reply.ConflictIndex-1, 0)
	} else {
		rf.NextIndex[server] = max(rf.NextIndex[server]-1, 1)
		rf.MatchIndex[server] = max(rf.MatchIndex[server]-1, 0)
	}
	// --- DEBUG STATEMENTS ---
	fmt.Println("AppendEntries: NextIndex is now: ", rf.NextIndex[server], "MatchIndex is now: ", rf.MatchIndex[server], "Log length is: ", len(rf.Log), "Conflict Index is: ", reply.ConflictIndex)
	fmt.Println("rf.me is ", rf.me, " and the server is ", server)
}

// -------------------------------------------------------------------------------------------------
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Call() sends a request and waits, if the reply comes with a timeout, its true otherwise Call() returns false
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	retries := 3
	for !ok && retries > 0 {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		retries--
		time.Sleep(50 * time.Millisecond)
	}

	// --- RPC failure ---
	//Either send successful empty appendEntries or fail in sending the rpc call
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	if reply.Term < rf.CurrentTerm {
		return
	}

	// --- TERM CHECKS ---
	if rf.handleTerm(reply) {
		return
	}

	// --- Successful Reply/Log Consistency ---
	if reply.Success {
		rf.handleSuccessfulReply(server, args, reply)
	} else {
		rf.handleFailedReply(server, reply)
	}
	// --- COMMIT INDEX ----
	rf.updateCommitIndex()
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	errMsg := "encode error"

	if err := e.Encode(rf.CurrentTerm); err != nil {
		log.Fatal(errMsg)
		fmt.Println("Error in encoding current term")
	}
	if err := e.Encode(rf.VotedFor); err != nil {
		log.Fatal(errMsg)
		fmt.Println("Error in encoding voted for")
	}
	if err := e.Encode(rf.Log); err != nil {
		log.Fatal(errMsg)
		fmt.Println("Error in encoding log")
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogItem
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil {
		log.Fatal("decode error")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
	}
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	// ------------------------------------- TERM Checks ------------------------------------------------
	if args.Term < rf.CurrentTerm {
		fmt.Println("Request Vote: Leader lags behind server")
		rf.VotedFor = -1
		rf.persist()
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.ServerState = "follower"
		rf.VotedFor = -1
		fmt.Println("Request Vote: current server is behind leader. Server voting has a Term - ", rf.CurrentTerm)
	}
	// ------------------------------------- LOG Consistency ------------------------------------------------
	lastIndex, lastTerm := args.LastLogIndex, args.LastLogTerm
	currLastIndex, currLastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	var storeTrue bool
	storeTrue = lastTerm > currLastTerm
	// if the last term of the candidate is same as the current server's last term, then the last index of the candidate is compared
	isLastTermUpdated := lastTerm == currLastTerm
	if isLastTermUpdated {
		// if the last index of the candidate is greater than the current server's last index, then the candidate is more up to date
		storeTrue = lastIndex >= currLastIndex
	}
	// if the server has not voted for anyone or if the server has voted for the candidate, then the server votes for the candidate
	// Also the candidate's log is at least as up to date as the server's log
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && storeTrue {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartBeat = true
		fmt.Println("Request Vote: Server voted for: ", args.CandidateId)
	}
	rf.persist()
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

	fmt.Println("startNewElection: Starting new election")
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.ServerState = "candidate"
	term, id, peers := rf.CurrentTerm, rf.me, rf.peers
	lastIndex, lastTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	rf.VotedFor = id
	rf.persist()
	rf.mu.Unlock()

	var votesRecieved int32 = 0

	for server := range peers {
		if server == id {
			atomic.AddInt32(&votesRecieved, 1)
			continue
		}

		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  id,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		}

		reply := RequestVoteReply{}

		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.sendRequestVote(server, args, reply)
			rf.mu.Lock()

			if !ok {
				rf.mu.Unlock()
				fmt.Println("startNewElection: request vote is false")
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.ServerState = "follower"
				rf.VotedFor = -1
				rf.CurrentTerm = reply.Term
				atomic.StoreInt32(&votesRecieved, 0)
				rf.persist()
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				atomic.AddInt32(&votesRecieved, 1)
				// fmt.Println("-----------------------------------------------")
				if atomic.LoadInt32(&votesRecieved) > int32(len(peers)/2) {
					rf.ServerState = "leader"
					votesRecieved = 0
					fmt.Println("----------------------------------")
					fmt.Println("NEW LEADER ELECTED: ", rf.me)
					rf.NextIndex, rf.MatchIndex = make([]int, len(peers)), make([]int, len(peers))
					lastIndex := rf.getLastLogIndex()
					for i := range peers {
						if i != rf.me {
							rf.NextIndex[i] = lastIndex + 1
							rf.MatchIndex[i] = 0
						}
					}
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
	for !rf.killed() {
		// The newly elected leader sends heartbeats through the AppendEntries Rpc which is empty
		// time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		serverState, term, id, peers, leaderCommit := rf.ServerState, rf.CurrentTerm, rf.me, rf.peers, rf.CommitIndex
		rf.mu.Unlock()

		if serverState == "leader" {
			fmt.Println("Server - ", id, " is starting Heartbeats!")
			for server := range peers {
				if server == id {
					continue
				}

				rf.mu.Lock()
				prevLogIndex := rf.NextIndex[server] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 && prevLogIndex < len(rf.Log) {
					prevLogTerm = rf.Log[prevLogIndex].Term
				}
				// entries := rf.Log[prevLogIndex+1:]
				entries := make([]LogItem, 0)
				if prevLogIndex+1 >= 0 && prevLogIndex+1 < len(rf.Log) {
					entries = rf.Log[prevLogIndex+1:]
				}
				rf.mu.Unlock()

				args := &AppendEntriesArgs{
					Term:         term,
					LeaderID:     id,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: leaderCommit,
				}
				args.Entries = entries
				reply := &AppendEntriesReply{}

				go rf.sendAppendEntries(server, args, reply)
			}
			time.Sleep(120 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------------------------------------
// Helper function to set random election timeouts
func setRandomElectionTimer() time.Duration {
	definedTimeValue, randomTime := 850, 750
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
			// channel to limit concurrent elections
			select {
			case rf.electionTracker <- struct{}{}:
				// go func() {
				rf.startNewElection()
				<-rf.electionTracker
				// }()
			default:
				// election already in place
				continue
			}
			// rf.startNewElection()
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
		// Log:         make([]LogItem, 0),

		CommitIndex: 0,
		LastApplied: 0,

		NextIndex:  make([]int, len(peers)),
		MatchIndex: make([]int, len(peers)),

		ServerState:    "follower",
		CheckForLeader: false, // no leader in the beginning

		lastHeartBeat: false,
		applyCh:       applyCh,
	}
	rf.Log = append(rf.Log, LogItem{Term: 0, Command: nil})
	rf.electionTracker = make(chan struct{}, 1)
	applyCh <- ApplyMsg{CommandValid: false, Command: -1, CommandIndex: 0}

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()

	return rf
}

// ------------------------------------------------------------------   END  -------------------------------------------------------------------------
