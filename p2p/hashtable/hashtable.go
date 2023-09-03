package hashtable

import (
	"fmt"
	"sync"
)

type Node struct {
	ID           string
	IP           string
	Pinged       bool
	Findnoded    bool
	TCPConnected bool
}

type hashtable struct {
	hashtab map[string]Node
	mu      sync.Mutex
	Length  int
}

func NewHashTable() *hashtable {
	nodeMap := make(map[string]Node)
	return &hashtable{hashtab: nodeMap, Length: 0}
}

// 添加一个新节点
func (ht *hashtable) CreateNode(node Node) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.hashtab[node.ID] = node
	ht.Length++
	// nodeMap[node.ID] = node
}

// 更改已有节点信息
func (ht *hashtable) UpdateNode(updatedNode Node) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if _, found := ht.hashtab[updatedNode.ID]; !found {
		return fmt.Errorf("Node with ID %s not found", updatedNode.ID)
	}

	updatedNode.ID = ht.hashtab[updatedNode.ID].ID // Ensure ID consistency
	ht.hashtab[updatedNode.ID] = updatedNode
	return nil
}

// 确认哈希表中是否存在特定id的某节点
func (ht *hashtable) Matchnode(id string) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if _, found := ht.hashtab[id]; found {
		return true // 存在
	}

	// return fmt.Errorf("Node with ID %s not found", id)
	return false // 不存在
}

// 打印哈希表信息
func (ht *hashtable) Printtable() {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	fmt.Println("Node Information:")
	for id, node := range ht.hashtab {
		fmt.Printf("ID: %s\n", id)
		fmt.Printf("IP: %s\n", node.IP)
		fmt.Printf("Findnoded: %v\n", node.Findnoded)
		fmt.Printf("Pinged: %v\n", node.Pinged)
		fmt.Printf("TCPConnected: %v\n", node.TCPConnected)
		fmt.Println("-------------")
	}
}

// func main() {
// 	// 创建一个存储Node的哈希表
// 	nodeMap := make(map[int]Node)

// 	// 添加节点到哈希表
// 	nodeMap[1] = Node{ID: 1, IP: "192.168.1.1", Name: "Node1"}
// 	nodeMap[2] = Node{ID: 2, IP: "192.168.1.2", Name: "Node2"}

// 	// 修改节点属性
// 	nodeMap[1] = Node{ID: 1, IP: "192.168.1.1", Name: "Node1", Pinged: true, TCPConnected: true}

// 	// 并发安全的访问哈希表
// 	var mu sync.Mutex
// 	go func() {
// 		mu.Lock()
// 		defer mu.Unlock()

// 		// 进行哈希表的读写操作
// 		node := nodeMap[1]
// 		node.Pinged = true
// 		nodeMap[1] = node
// 	}()

// 	go func() {
// 		mu.Lock()
// 		defer mu.Unlock()

// 		// 进行哈希表的读写操作
// 		node := nodeMap[2]
// 		node.TCPConnected = true
// 		nodeMap[2] = node
// 	}()

// 	// 输出节点属性
// 	fmt.Println(nodeMap[1])
// 	fmt.Println(nodeMap[2])
// }
