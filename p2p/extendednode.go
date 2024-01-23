package p2p

import (
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
)

// ExtendedNodeInfo 存储关于节点的扩展信息
type ExtendedNodeInfo struct {
	Node        *enode.Node // 节点信息
	LastConnect time.Time   // 上一次成功连接的时间
	Handshaked  bool        // 是否完成了eth握手
}

// ExtendedNodeStorage 用于存储ExtendedNodeInfo的内存结构
type ExtendedNodeStorage struct {
	nodes map[string]*ExtendedNodeInfo
	lock  sync.RWMutex // 用于保护nodes的读写锁
}

// NewExtendedNodeStorage 创建一个新的ExtendedNodeStorage实例
func NewExtendedNodeStorage() *ExtendedNodeStorage {
	return &ExtendedNodeStorage{
		nodes: make(map[string]*ExtendedNodeInfo),
	}
}

// AddOrUpdateNode 添加或更新节点信息
func (s *ExtendedNodeStorage) AddOrUpdateNode(node *enode.Node, handshaked bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	id := node.ID().String()
	info, exists := s.nodes[id]
	if !exists {
		info = &ExtendedNodeInfo{Node: node}
		s.nodes[id] = info
	}
	info.LastConnect = time.Now()
	info.Handshaked = handshaked
}

// GetNode 返回与给定ID关联的节点信息，如果找不到则返回nil
func (s *ExtendedNodeStorage) GetNode(id string) *ExtendedNodeInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()

	info, exists := s.nodes[id]
	if !exists {
		return nil
	}
	return info
}

// GetAllEnodes 返回所有存储的节点
func (s *ExtendedNodeStorage) GetAllEnodes() []*enode.Node {
	s.lock.RLock()
	defer s.lock.RUnlock()

	allEnodes := make([]*enode.Node, 0, len(s.nodes))
	for _, info := range s.nodes {
		allEnodes = append(allEnodes, info.Node)
	}
	return allEnodes
}

// GetAllNodes 返回所有存储的节点信息
func (s *ExtendedNodeStorage) GetAllNodesInfo() []*ExtendedNodeInfo {
	s.lock.RLock()
	defer s.lock.RUnlock()

	allNodes := make([]*ExtendedNodeInfo, 0, len(s.nodes))
	for _, info := range s.nodes {
		allNodes = append(allNodes, info)
	}
	return allNodes
}

// RemoveNode 删除指定节点的记录
func (s *ExtendedNodeStorage) RemoveNode(node *enode.Node) {
	s.lock.Lock()
	defer s.lock.Unlock()

	id := node.ID().String()
	delete(s.nodes, id)
}

// NodeCount 返回存储中的节点数
func (s *ExtendedNodeStorage) NodeCount() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.nodes)
}

// GetEarliestConnectedEnodes 返回最早连接的n个enode.Node
func (s *ExtendedNodeStorage) GetEarliestConnectedEnodes(n int) []*enode.Node {
	s.lock.RLock()
	defer s.lock.RUnlock()

	// 复制所有节点信息到一个新的切片
	allNodesInfo := make([]*ExtendedNodeInfo, 0, len(s.nodes))
	for _, info := range s.nodes {
		allNodesInfo = append(allNodesInfo, info)
	}

	// 根据 LastConnect 字段对节点进行排序
	sort.Slice(allNodesInfo, func(i, j int) bool {
		return allNodesInfo[i].LastConnect.Before(allNodesInfo[j].LastConnect)
	})

	// 如果请求的节点数超过实际节点数，调整返回数量
	if n > len(allNodesInfo) {
		n = len(allNodesInfo)
	}

	// 从排序后的节点信息中提取出 enode.Node 对象
	earliestNodes := make([]*enode.Node, n)
	for i := 0; i < n; i++ {
		earliestNodes[i] = allNodesInfo[i].Node
	}

	return earliestNodes
}

// CountHandshakedNodes 返回 Handshaked 为 true 的节点数量
func (s *ExtendedNodeStorage) CountHandshakedNodes() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	count := 0
	for _, info := range s.nodes {
		if info.Handshaked {
			count++
		}
	}
	return count
}