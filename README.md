1) Raft is a distributed consensus algorithm where multiple nodes have to agree with each other.

2) Each node is a:
    - Follower
    - Candidate
    - Leader

3) In the beginning every node is a follower and there is no leader.
    Then there is an election where one leader is elected (150ms to 300ms)
    There is an election timer that expires, and a follower becomes a candidate, votes for itself and asks for a vote
    from every other node. Then each node, if they havent voted in the election cycle, vote again. The node then resets the timer.

4) Once the leader is elected, they send AppendEntries messages to its followers.

5) Once a leader is elected, all changes go through the leader. Changes are added as an entry to the log of the leader however until a majority is reached, they are not implemented. Once there is consensus, the leader notifies that the entry has been committed. Entry is committed and node state is updated.

6) The AppendEntries are sent as heartbeats

7) Election term continues until followers stop recieving heartbeats and becomes a candidate

8) There is a case for a split vote (lets say there are 2 clients and 5 nodes). There will be 3 that recieve a majority and the network barrier is broken. 

9) For log replication, after the leader recieved a majority from the replies of the AppendEntries, it commits the changes


-----------------------------------------------------------------------------------------------------------

Leader Election
 
- 2 timeout settings
    election timeout is the time a follower waits before becoming a candidate (rand b/w 150ms and 300ms)
    Node votes for itself and asks for votes (Request Vote)
    if requested node hasnt voted in this term, it votes again and node resets the election timeout

    Leader is elected from the majority and sends an AppendEntries message to its followers. These messages are sent as heartbeats

    Followers respond to each respondEntries message. Election term till followers stops recieveing heartbeats. Then stoo leader and re-election

- In case of split vote, two nodes send messages at the same time. Each candidate has 2 votes and no more for this term. Nodes wait for a new election

Leader election
The leader periodically sends a heartbeat to its followers to maintain authority. A leader election is triggered when a follower times out after waiting for a heartbeat from the leader. This follower transitions to the candidate state and increments its term number. After voting for itself, it issues RequestVotes RPC in parallel to others in the cluster. Three outcomes are possible:

The candidate receives votes from the majority of the servers and becomes the leader. It then sends a heartbeat message to others in the cluster to establish authority.
If other candidates receive AppendEntries RPC, they check for the term number. If the term number is greater than their own, they accept the server as the leader and return to follower state. If the term number is smaller, they reject the RPC and still remain a candidate.
The candidate neither loses nor wins. If more than one server becomes a candidate at the same time, the vote can be split with no clear majority. In this case a new election begins after one of the candidates times out.
-----------------------------------------------------------------------------------------------------------

Log Replication

- Request sent by leader for AppendEntries. Client sends the change to the leader, which commits after a majority is recieved, response sent to the client
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------


State Variables

All servers:
- currentTerm: latest term server has seen (start at 0)
- votedFor   : candidate id last elected
- log[]      : log entries 

Volatile on servers:
- commitIndex : index of the last log committed on the server
- lastApplied : index of last log applied to the state machine

Volatile on leaders:
- nextIndex[] : index to be sent to servers
- matchIndex[]: index to be replicated across servers

AppliedEntries RPC:

- term, leaderId, prevLogTerm, prevLogIndex, leaderIndex, entries[]
false if term < currentTerm
false if prevLogTerm at prevLogIndex is not there
if logterm is different at the same index, delete it and all that follow
if leaderCommit > commitIndex, commitIndex = min(leaderCommit, index of last log applied)
append to the logs of servers

RequestVote Rpc:
- term, candidateId, lastLogTerm, lastLogIndex
false if term < currentTerm
if votedFor is null or candidateId and cadidate's log as up to date, grant vote

Conditions for all servers
if term t > currentTerm, convert to follower and currentterm = t
if commitIndex>lastApplied, lastApplied = commitIndex

followers
- reply to candidates and leaders
- if no heartbeat and election timeout runs out, become candidate

candidates
- when becomes candidate
    - increment currentTerm
    - vote for self
    - send RequestVote Rpc
    - reset election timer
- if recieved majority, new leader
- if appendEntries RPC recieved, become follower
- if no result, start new election

leaders
- when becomes leader send out an empty AppendEntries Rpc and do so during idle periods as well
- if lastIndex >= nextIndex send AppendEntries RPC
    - if successfull update nextIndex and matchIndex
    - else decrement nextIndex and try again
- if command recieved from client, append entry to local log, respond after entry applied to state machine
- if there exists an N such that N >= commitIndex, a majority of matchIndex[i] > N and log[N].term == currentTerm,
set commitIndex = N