package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

//import "fmt"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	LogTerm int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state         State
	chanGrantVote chan bool
	chanHeartBeat chan bool
	chanLeader    chan bool
	chanCommit    chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && args.LastLogTerm >= rf.log[len(rf.log)-1].LogTerm && args.LastLogIndex >= len(rf.log)-1 {
		rf.chanGrantVote <- true
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.state = Follower
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

//
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
// Call() returns false.fr Thus Call() may not return for a while.
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	ReplyIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//fmt.Printf("args.Term < rf.currentTerm\n")
		//fmt.Printf(" reply.Term = %v\n", reply.Term)
		//fmt.Printf(" args.Term = %v\n", args.Term)
		//fmt.Printf(" rf.currentTerm = %v\n", rf.currentTerm)
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.chanHeartBeat <- true

	if len(rf.log) <= args.PrevLogIndex {
		//fmt.Printf("scenario 2 fasle\n")
		reply.Success = false
		reply.ReplyIndex = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].LogTerm != args.PrevLogTerm {
		//fmt.Printf("scenario 3 false\n")
		reply.Success = false
		for i := args.PrevLogIndex - 1; i > 0; i-- {
			if rf.log[i].LogTerm == args.PrevLogTerm {
				reply.ReplyIndex = i
				break
			}
		}
		if reply.ReplyIndex < 1 {
			reply.ReplyIndex = 1
		}
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	for _, logEntry := range args.Entries {
		rf.log = append(rf.log, logEntry)
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true

	}
	reply.Success = true
	reply.ReplyIndex = len(rf.log)
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("ok = %v\n", ok)
	//return ok

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != Leader {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = len(rf.log)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.ReplyIndex
		}
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	// Your code here (2B).
	if isLeader {
		logEntry := LogEntry{term, command}
		rf.log = append(rf.log, logEntry)
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanHeartBeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanCommit = make(chan bool, 100)
	rf.log = append(rf.log, LogEntry{LogTerm: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0
	//rf.nextIndex = make([]int, len(peers))
	//rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case Follower:
				//fmt.Printf("%v is Follower\n", rf.me)
				select {
				case <-rf.chanHeartBeat:
					//fmt.Printf("   %v recieved HeartBeat\n", rf.me)
				case <-rf.chanGrantVote:
					//fmt.Printf("   %v recieved GrantVote\n", rf.me)
				case <-time.After(time.Duration(rand.Intn(500)+500) * time.Millisecond):
					rf.state = Candidate
				}
			case Leader:
				//fmt.Printf("%v is Leader\n", rf.me)
				rf.mu.Lock()
				//defer rf.mu.Unlock()

				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderID = rf.me

				rf.mu.Unlock()
				//fmt.Printf("CurrentTerm = %v\n", rf.currentTerm)

				for i := range rf.peers {
					if i != rf.me && rf.state == Leader {

						go func(i int) {
							//cont := true
							//for cont == true {
							rf.mu.Lock()
							//defer rf.mu.Unlock()
							args.PrevLogIndex = rf.nextIndex[i] - 1
							args.PrevLogTerm = rf.log[rf.nextIndex[i]-1].LogTerm

							args.Entries = rf.log[rf.nextIndex[i]:len(rf.log)]
							//fmt.Printf("args.Entries = %v\n", args.Entries)
							//args.LeaderCommit = rf.commitIndex

							N := rf.commitIndex
							for i := rf.commitIndex + 1; i < len(rf.log); i++ {
								count := 1
								for j := range rf.peers {
									if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].LogTerm == rf.currentTerm {
										count += 1
									}
									if 2*count > len(peers) {
										N = i
									}
								}
							}
							if N != rf.commitIndex {
								rf.commitIndex = N
								rf.chanCommit <- true
							}

							args.LeaderCommit = rf.commitIndex
							rf.mu.Unlock()
							var reply AppendEntriesReply
							rf.sendAppendEntries(i, args, &reply)

							/*
								if reply.Success == false {
									//fmt.Printf("appendentries failed\n")
									if reply.Term > rf.currentTerm {
										rf.mu.Lock()
										fmt.Printf("WORKING PROPERLY\n")
										fmt.Printf(" reply.Term = %v\n", reply.Term)
										fmt.Printf(" args.Term = %v\n", args.Term)
										fmt.Printf(" rf.currentTerm = %v\n", rf.currentTerm)
										//rf.mu.Lock()
										rf.currentTerm = reply.Term
										rf.state = Follower
										rf.votedFor = -1
										cont = false
										rf.mu.Unlock()
									}
									if reply.Term != 0 {
										rf.mu.Lock()
										fmt.Printf("do we get here\n")
										fmt.Printf(" reply.Term = %v\n", reply.Term)
										//fmt.Printf(" args.Term = %v\n", args.Term)
										fmt.Printf(" rf.currentTerm = %v\n", rf.currentTerm)
										rf.nextIndex[i] -= 1
										cont = false
										rf.mu.Unlock()
									}
								} else {
									rf.mu.Lock()
									rf.nextIndex[i] = len(rf.log)
									rf.matchIndex[i] = rf.nextIndex[i] - 1
									cont = false
									rf.mu.Unlock()
								}
							}*/
						}(i)
					}
				}
				time.Sleep(200 * time.Millisecond)
			case Candidate:
				rf.mu.Lock()

				//fmt.Printf("%v is Candidate\n", rf.me)

				//fmt.Printf("CandidateTerm is %v\n", rf.currentTerm)
				rf.currentTerm++
				//fmt.Printf("CandidateTerm POST UPDATE is %v\n", rf.currentTerm)

				rf.votedFor = rf.me
				rf.persist()

				var voteCount int = 1

				var args RequestVoteArgs
				args.Term = rf.currentTerm
				args.CandidateID = rf.me
				args.LastLogIndex = len(rf.log) - 1
				args.LastLogTerm = rf.log[len(rf.log)-1].LogTerm

				rf.mu.Unlock()
				for i := range rf.peers {
					if i != rf.me && rf.state == Candidate {
						go func(i int) {
							//rf.mu.Lock()
							//defer rf.mu.Unlock()

							var reply RequestVoteReply
							rf.sendRequestVote(i, args, &reply)

							//fmt.Printf("  sent vote request to %v\n", i)

							if reply.VoteGranted == true {
								//fmt.Printf("  vote granded from %v\n", i)
								rf.mu.Lock()
								voteCount++
								if voteCount > len(rf.peers)/2 && rf.state == Candidate {
									rf.chanLeader <- true
								}
								rf.mu.Unlock()
							} else if reply.Term > rf.currentTerm {
								rf.mu.Lock()
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.persist()
								rf.mu.Unlock()
							}
							//rf.mu.Unlock()
						}(i)
					}
				}
				select {
				case <-time.After(time.Duration(rand.Intn(500)+500) * time.Millisecond):
				case <-rf.chanHeartBeat:
					rf.mu.Lock()
					rf.state = Follower
					rf.mu.Unlock()
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}
