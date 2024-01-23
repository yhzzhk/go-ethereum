package neo4j

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	uri      = "bolt://localhost:7687"
	username = "neo4j"
	password = "11111111"
)

type cqlconnection struct {
	uri      string
	username string
	password string
}

func NewCQLConnection(ctx context.Context) *cqlconnection {

	cn := &cqlconnection{
		uri:      uri,
		username: username,
		password: password,
	}
	return cn
}

// func (cn *cqlconnection) CreateNode(ctx context.Context, label string, properties map[string]interface{}) (string, error) {
// 	// fmt.Println("neo4j数据库: Createnode")
// 	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
// 	if err != nil {
// 		return "", err
// 	}
// 	defer driver.Close(ctx)

// 	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
// 	defer session.Close(ctx)

// 	cql := "CREATE (n:" + label + " $props) RETURN n.id"
// 	node, err := session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
// 		result, err := transaction.Run(ctx, cql, map[string]any{"props": properties})
// 		if err != nil {
// 			return "", err
// 		}

// 		if result.Next(ctx) {
// 			return result.Record().Values[0], nil
// 		}

// 		return "", result.Err()
// 	})
// 	if err != nil {
// 		return "", err
// 	}
// 	return node.(string), nil
// }

// func (cn *cqlconnection) UpdateNode(ctx context.Context, id string, properties map[string]interface{}) error {
	
// 	// fmt.Println("neo4j数据库: Updatenode: ", id)
// 	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
// 	if err != nil {
// 		return err
// 	}
// 	defer driver.Close(ctx)

// 	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
// 	defer session.Close(ctx)

// 	cql := "MATCH (n) WHERE n.id = $id SET n += $props RETURN n.id"
// 	_, err = session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
// 		_, err := transaction.Run(ctx, cql, map[string]any{"id": id, "props": properties})
// 		return nil, err
// 	})

// 	return err
// }

func (cn *cqlconnection) DeleteNode(ctx context.Context, id string) error {
	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	cql := "MATCH (n) WHERE n.id = $id DELETE n"
	_, err = session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		_, err := transaction.Run(ctx, cql, map[string]any{"id": id})
		return nil, err
	})

	return err
}

func (cn *cqlconnection) UpsertNode(ctx context.Context, id string, properties map[string]interface{}) (string, error) {
	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 检查节点是否存在
	exists, err := cn.CheckNodeExists(ctx, id)
	if err != nil {
		return "", err
	}

	var cql string
	var re string
	if exists {
		// 更新节点
		cql = "MATCH (n) WHERE n.id = $id SET n += $props RETURN n.id"
		re = "neo4j更新节点:" + id
	} else {
		// 创建节点
		cql = "CREATE (n:Node $props) RETURN n.id"
		re = "neo4j创建节点:" + id
	}

	_, err = session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		_, err := transaction.Run(ctx, cql, map[string]any{"id": id, "props": properties})
		return nil, err
	})

	return re, err
}

func (cn *cqlconnection) CheckNodeExists(ctx context.Context, id string) (bool, error) {
	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return false, err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	cql := "MATCH (n) WHERE n.id = $id RETURN count(n) > 0"
	exists, err := session.ExecuteRead(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx, cql, map[string]any{"id": id})
		if err != nil {
			return false, err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}

		return false, result.Err()
	})
	if err != nil {
		return false, err
	}
	return exists.(bool), nil
}