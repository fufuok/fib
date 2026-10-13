// Package udpbatch reads and writes UDP datagrams many to a system call:
// recvmmsg and sendmmsg on Linux, recvmsg_x and sendmsg_x on macOS. On the
// other BSDs it moves one datagram per call, and on other systems only
// MaxDatagramSize is defined.
package udpbatch

// MaxDatagramSize holds the largest UDP payload, so no datagram is ever
// truncated on the way in.
const MaxDatagramSize = 64 << 10
