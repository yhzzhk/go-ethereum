// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package discover

import (
	"context"
	"errors"
	"time"
	"fmt"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/neo4j"
)

// lookup performs a network search for nodes close to the given target. It approaches the
// target by querying nodes that are closer to it on each iteration. The given target does
// not need to be an actual node identifier.
type lookup struct {
	tab         *Table
	queryfunc   func(*node) ([]*node, error)
	replyCh     chan []*node
	cancelCh    <-chan struct{}
	asked, seen map[enode.ID]bool
	result      nodesByDistance
	replyBuffer []*node
	queries     int
}

type queryFunc func(*node) ([]*node, error)

func newLookup(ctx context.Context, tab *Table, target enode.ID, q queryFunc) *lookup {
	it := &lookup{
		tab:       tab,
		queryfunc: q,
		asked:     make(map[enode.ID]bool),
		seen:      make(map[enode.ID]bool),
		result:    nodesByDistance{target: target},
		replyCh:   make(chan []*node, alpha),
		cancelCh:  ctx.Done(),
		queries:   -1,
	}
	// Don't query further if we hit ourself.
	// Unlikely to happen often in practice.
	it.asked[tab.self().ID()] = true
	return it
}

// run runs the lookup to completion and returns the closest nodes found.
func (it *lookup) run() []*enode.Node {
	for it.advance() {
	}
	return unwrapNodes(it.result.entries)
}

// advance advances the lookup until any new nodes have been found.
// It returns false when the lookup has ended.
func (it *lookup) advance() bool {
	for it.startQueries() {
		select {
		case nodes := <-it.replyCh:
			it.replyBuffer = it.replyBuffer[:0]
			for _, n := range nodes {
				if n != nil && !it.seen[n.ID()] {
					it.seen[n.ID()] = true
					it.result.push(n, bucketSize)
					it.replyBuffer = append(it.replyBuffer, n)
				}
			}
			it.queries--
			if len(it.replyBuffer) > 0 {
				return true
			}
		case <-it.cancelCh:
			it.shutdown()
		}
	}
	return false
}

func (it *lookup) shutdown() {
	for it.queries > 0 {
		<-it.replyCh
		it.queries--
	}
	it.queryfunc = nil
	it.replyBuffer = nil
}

func (it *lookup) startQueries() bool {
	if it.queryfunc == nil {
		return false
	}

	// The first query returns nodes from the local table.
	if it.queries == -1 {
		closest := it.tab.findnodeByID(it.result.target, bucketSize, false)
		// Avoid finishing the lookup too quickly if table is empty. It'd be better to wait
		// for the table to fill in this case, but there is no good mechanism for that
		// yet.
		if len(closest.entries) == 0 {
			it.slowdown()
		}
		it.queries = 1
		it.replyCh <- closest.entries
		return true
	}

	// Ask the closest nodes that we haven't asked yet.
	for i := 0; i < len(it.result.entries) && it.queries < alpha; i++ {
		n := it.result.entries[i]
		if !it.asked[n.ID()] {
			it.asked[n.ID()] = true
			it.queries++
			go it.query(n, it.replyCh)
		}
	}
	// The lookup ends when no more nodes can be asked.
	return it.queries > 0
}

func (it *lookup) slowdown() {
	sleep := time.NewTimer(1 * time.Second)
	defer sleep.Stop()
	select {
	case <-sleep.C:
	case <-it.tab.closeReq:
	}
}

func (it *lookup) query(n *node, reply chan<- []*node) {
	fails := it.tab.db.FindFails(n.ID(), n.IP())
	r, err := it.queryfunc(n)
	if errors.Is(err, errClosed) {
		// Avoid recording failures on shutdown.
		reply <- nil
		return
	} else if len(r) == 0 {
		fails++
		it.tab.db.UpdateFindFails(n.ID(), n.IP(), fails)
		// Remove the node from the local table if it fails to return anything useful too
		// many times, but only if there are enough other nodes in the bucket.
		dropped := false
		if fails >= maxFindnodeFailures && it.tab.bucketLen(n.ID()) >= bucketSize/2 {
			dropped = true
			it.tab.delete(n)
		}
		it.tab.log.Trace("FINDNODE failed", "id", n.ID(), "failcount", fails, "dropped", dropped, "err", err)
	} else if fails > 0 {
		// Reset failure counter because it counts _consecutive_ failures.
		it.tab.db.UpdateFindFails(n.ID(), n.IP(), 0)
	}

	// 创建 cqlconnection 实例
	ctx := context.Background()
	conn := neo4j.NewCQLConnection(ctx)

	// 处理交互节点 n
	interactionNodeId := n.ID().String()
	interactionNodeIp := n.IP().String()
	interactionNodeUdpPort := n.UDP() // 假设这是获取UDP端口的方式
	interactionNodeTcpPort := n.TCP() // 假设这是获取TCP端口的方式
	interactionNodeEnr := n.String()  // 假设这是获取ENR的方式

	// 更新或创建交互节点，并将 iffindnode 设置为 true
	_, err = conn.CreateOrUpdateNode(ctx, interactionNodeId, interactionNodeIp, interactionNodeUdpPort, interactionNodeTcpPort, true, interactionNodeEnr)
	if err != nil {
		// 如果发生错误，记录并处理
		fmt.Printf("Failed to create or update interaction node: %v\n", err)
		reply <- nil
		return
	}

	// 如果交互节点已存在，删除其所有现有的向外关系
	err = conn.DeleteExistingRelations(ctx, interactionNodeId)
	if err != nil {
		// 如果发生错误，记录并处理
		fmt.Printf("Failed to delete existing relations for interaction node: %v\n", err)
		reply <- nil
		return
	}

	// 处理邻居节点列表 r
	for _, neighbor := range r {
		neighborId := neighbor.ID().String()
		neighborIp := neighbor.IP().String()
		neighborUdpPort := neighbor.UDP()
		neighborTcpPort := neighbor.TCP()
		neighborEnr := neighbor.String()
		// 这里需要一种方法来计算或获取两个节点之间的距离
		neighborDistance := enode.LogDist(n.ID(), neighbor.ID()) - 239

		// 更新或创建邻居节点，并将 iffindnode 设置为 false
		_, err = conn.CreateOrUpdateNode(ctx, neighborId, neighborIp, neighborUdpPort, neighborTcpPort, false, neighborEnr)
		if err != nil {
			// 如果发生错误，记录并继续处理下一个邻居节点
			fmt.Printf("Failed to create or update neighbor node: %v\n", err)
			continue
		}

		// 创建从交互节点到邻居节点的关系
		err = conn.CreateRelation(ctx, interactionNodeId, neighborId, neighborDistance)
		if err != nil {
			// 如果发生错误，记录并继续处理下一个邻居节点
			fmt.Printf("Failed to create relation: %v\n", err)
			continue
		}
	}


	// Grab as many nodes as possible. Some of them might not be alive anymore, but we'll
	// just remove those again during revalidation.
	for _, n := range r {
		it.tab.addSeenNode(n)
	}
	reply <- r
}

// lookupIterator performs lookup operations and iterates over all seen nodes.
// When a lookup finishes, a new one is created through nextLookup.
type lookupIterator struct {
	buffer     []*node
	nextLookup lookupFunc
	ctx        context.Context
	cancel     func()
	lookup     *lookup
}

type lookupFunc func(ctx context.Context) *lookup

func newLookupIterator(ctx context.Context, next lookupFunc) *lookupIterator {
	ctx, cancel := context.WithCancel(ctx)
	return &lookupIterator{ctx: ctx, cancel: cancel, nextLookup: next}
}

// Node returns the current node.
func (it *lookupIterator) Node() *enode.Node {
	if len(it.buffer) == 0 {
		return nil
	}
	return unwrapNode(it.buffer[0])
}

// Next moves to the next node.
func (it *lookupIterator) Next() bool {
	// Consume next node in buffer.
	if len(it.buffer) > 0 {
		it.buffer = it.buffer[1:]
	}
	// Advance the lookup to refill the buffer.
	for len(it.buffer) == 0 {
		if it.ctx.Err() != nil {
			it.lookup = nil
			it.buffer = nil
			return false
		}
		if it.lookup == nil {
			it.lookup = it.nextLookup(it.ctx)
			continue
		}
		if !it.lookup.advance() {
			it.lookup = nil
			continue
		}
		it.buffer = it.lookup.replyBuffer
	}
	return true
}

// Close ends the iterator.
func (it *lookupIterator) Close() {
	it.cancel()
}
