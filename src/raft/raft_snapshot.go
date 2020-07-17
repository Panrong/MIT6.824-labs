package raft

// InstallSnapshot RPC
// Invoked by leader to send chunks of a snapshot to a follower
// leaders always send chunk in order

type InstallSnapshotArgs struct {
	Term             int      // leader's term
	LeaderId         int      // so follower can redirect clients
	LastIncludeIndex int      // the snapshot replaces all entries up through and including this index
	LastIncludeTerm  int      // term of LastIncludeIndex
	Data             []byte   // raw bytes of the snapshot chunk
}

type InstallSnapshotReply struct {
	Term int  // currentTerm, for leader to update itself
}

// InstallSnapshot RPC Receiver implementation
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

}

func (rf *Raft) sendInstallSnapshot(peerIdx int) {

}

func (rf *Raft) SavePersistAndSnapshot(logIndex int, snapshotData []byte) {

}