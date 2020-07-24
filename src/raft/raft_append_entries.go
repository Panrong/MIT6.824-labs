package raft

import (
	"time"
)

// AppendEntries RPC
// Invoke by leader to replicate log entries
// also used as heartbeat

type AppendEntriesArgs struct {
	Term         int         // leader's term
	LeaderId     int         // so followers can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of PrevLogIndex entry
	Entries      []LogEntry  // log entries to store (empty for heartbeat, may send more thant one for efficiency)
	LeaderCommit int         // leader's commitIndex
}

type AppendEntriesReply struct {
	Term      int    // currentTerm, for leader to update itself
	Success   bool   // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	NextIndex int
}

// AppendEntries RPC receiver implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("append entries")
	rf.log("get append entries:%+v", *args)

	// reply currentTerm
	reply.Term = rf.term

	// reply false if leader's term < currentTerm (Paper section 5.1)
	if rf.term > args.Term {
		rf.unlock("append entries")
		return
	}

	// renew lease with the leader
	rf.term = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	// apply entries
	// 1. reply false if log does not contain an entry at PrevLogIndex whose term matches PrevLogTerm (paper section 5.3)
	// 2. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (paper section 5.3)
	// 3. apply any new entries not already in the log
	_, lastLogIndex := rf.lastLogTermIndex()
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		// leader发来的PrevLogIndex < lastSnapshotIndex，理论上lastSnapshotIndex应该已经被当前server apply到state machine了，正常情况不该发生
		reply.Success = false
		reply.NextIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		// leader发来的PrevLogIndex > lastLogIndex，说明当前server缺少中间的log
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		// 上一个刚好是快照
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:1], args.Entries...) // 保留 logs[0]
			reply.NextIndex = rf.getNextIndex()
		}
	} else if rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term == args.PrevLogTerm {
		// 包括刚好是后续的 log 和需要删除部分两种情况
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[0:rf.getRealIdxByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}
	} else {
		rf.log("prev log not match")
		reply.Success = false
		// 尝试跳过一个 term
		term := rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logEntries[rf.getRealIdxByLogIndex(idx)].Term == term {
			idx -= 1
		}
		reply.NextIndex = idx + 1
	}

	if reply.Success {
		// set commitIndex = min(LeaderCommit, index of last new entry)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct {}{}
		}
	}

	rf.persist()
	rf.log("get append entries:%+v, reply:%+v", *args, *reply)
	rf.unlock("append entries")
}

func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	if (argsLastIndex < lastIndex) && (args.Term == lastTerm) {
		return true
	}
	return false
}


func (rf *Raft) getAppendLogs(peerIdx int) (prevLogIndex, prevLogTerm int, res []LogEntry) {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if (nextIdx < rf.lastSnapshotIndex) || (nextIdx > lastLogIndex) {
		// 没有需要发送的log
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	res = append([]LogEntry{}, rf.logEntries[rf.getRealIdxByLogIndex(nextIdx):]...)
	prevLogIndex = nextIdx - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	}
	return
}

func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	prevLogIndex, prevLogTerm, logs := rf.getAppendLogs(peerIdx)
	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) resetHeartBeatTimer(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(HeartBeatTimeout)
}

func (rf *Raft) resetHeartBeatTimers() {
	for i, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.lock("append to peer 1")
		if rf.role != Leader {
			rf.resetHeartBeatTimer(peerIdx)
			rf.unlock("append to peer 1")
			return
		}

		args := rf.getAppendEntriesArgs(peerIdx)
		rf.resetHeartBeatTimer(peerIdx)
		rf.unlock("append to peer 1")

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(&args, &reply)

		select {
		case <- rf.stopCh:
			return
		case <- RPCTimer.C:
			rf.log("appendtopeer, rpctimeout: peer:%d, args:%+v", peerIdx, args)
			continue
		case ok := <- resCh:
			if !ok {
				rf.log("appendtopeer not ok")
				continue
			}
		}

		rf.log("appendtoperr, peer:%d, args:%+v, reply:%+v", peerIdx, args, reply)
		// call ok, check reply
		rf.lock("append to peer 2")

		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			rf.unlock("append to peer 2")
			return
		}

		if rf.role != Leader || rf.term != args.Term {
			rf.unlock("append to peer 2")
			return
		}

		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				// 只 commit 自己 term 的 index
				rf.updateCommitIndex()
			}
			rf.persist()
			rf.unlock("append to peer 2")
			return
		}

		// success == false
		if reply.NextIndex != 0 {
			if reply.NextIndex > rf.lastSnapshotIndex {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.unlock("append to peer 2")
				continue
				// need retry
			} else {
				// send sn rpc
				go rf.sendInstallSnapshot(peerIdx)
				rf.unlock("append to peer 2")
				return
			}
		} else {
			rf.unlock("append to peer 2")
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.log("in update commit index")
	hasCommit := false
	for i := rf.commitIndex + 1; i <= rf.lastSnapshotIndex + len(rf.logEntries); i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					rf.log("update commit index:%d", i)
					break
				}
			}
		}
		if rf.commitIndex != i {
			// 后续的不需要再判断
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}