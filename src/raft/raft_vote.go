package raft

import (
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)

	// 2A
	rf.lock("request vote")
	defer rf.unlock("request vote")
	defer func() {
		rf.log("get request vote, args:%+v, reply:%+v", args, reply)
	}()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.role == Leader {
			return
		}

		if rf.voteFor == args.CandidateId {
			// has voted for current server
			reply.VoteGranted = true
			return
		}

		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			// has voted for other server
			return
		}
	}

	defer rf.persist()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.changeRole(Follower)
	}

	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		// restrictions on the candidate:
		// in order to get votes, the candidate must:
		// either (1) has larger term
		// or (2) has same term but longer log index
		return
	}

	rf.term = args.Term
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	rf.log("vote for:%d", args.CandidateId)
	return
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVoteToPeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if ok == false {
				// 当网络出错call迅速返回时，会产生大量goroutine及rpc, 所以加了sleep
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <- t.C:
			return
		case <- rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.lock("start election")

	rf.electionTimer.Reset(randElectionTimeout())
	if rf.role == Leader {
		rf.unlock("start election")
		return
	}

	rf.log("start election")
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.persist()
	rf.unlock("start election")

	grantedCount := 1
	chResCount := 1

	votesCh := make(chan bool, len(rf.peers))
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVoteToPeer(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.lock("start election change term")
				if rf.term < reply.Term {
					rf.term = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("start election change term")
			}
		}(votesCh, index)
	}

	for {
		// count votes
		r := <-votesCh
		chResCount += 1
		if r == true {
			grantedCount += 1
		}
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	// win not enough votes
	if grantedCount <= len(rf.peers)/2 {
		rf.log("grantedCount <= len/2:count:%d", grantedCount)
		return
	}

	// win enough votes
	rf.lock("start election win")
	rf.log("before try change to leader,count:%d, args:%+v", grantedCount, args)
	if rf.term == args.Term && rf.role == Candidate {
		rf.changeRole(Leader)
		rf.persist()
	}
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	rf.unlock("start election win")

}