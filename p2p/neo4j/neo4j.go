package neo4j

import (
	"context"
	"os"
	// "fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (

	uri      = "bolt://localhost:7687"
	username = "neo4j"
	password = "testpassword"

)
// var (
// 	uri      = "bolt://neo4j:7687" // 直接使用服务名称
// 	username = os.Getenv("NEO4J_USERNAME")
// 	password = os.Getenv("NEO4J_PASSWORD")
//   )

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
	// fmt.println(uri)
	return cn
}

func(cn *cqlconnection) GetURI()(string){
	return cn.uri
}

func (cn *cqlconnection) CreatNode(ctx context.Context, id string, ip string) (string, error) {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	cql := "CREATE (a:exeNode {id:$id, ip:$ip}) RETURN a.id + ', from node ' + id(a) "

	node, err := session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
		result, err := transaction.Run(ctx,
			cql, map[string]any{"id": id, "ip": ip})
		if err != nil {
			return "", err
		}

		if result.Next(ctx) {
			return result.Record().Values[0], nil
		}

		return "", result.Err()
	})
	if err != nil {
		return "", err
	}
	return node.(string), nil
}

func (cn *cqlconnection) IfNodeIn(ctx context.Context, id string) (bool, error) {
	// 初始值设置为false
	r := false

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return r, err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead}) // 以只读模式创建session
	defer session.Close(ctx)

	// 定义Cypher查询语句
	cql := "MATCH (a:exeNode {id: $id}) RETURN a.id"

	// 执行查询
	result, err := session.Run(ctx, cql, map[string]interface{}{"id": id})
	if err != nil {
		return r, err
	}

	// 检查结果中是否存在记录
	if result.Next(ctx) {
		r = true
	}

	return r, result.Err()
}

// CreateEdge 用于在Neo4j数据库中创建两个节点之间的关系
func (cn *cqlconnection) CreateEdge(ctx context.Context, id1 string, id2 string, distance string) (string, error) {
	// 创建Neo4j driver
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	// 创建Neo4j session
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	// 使用defer确保session关闭
	defer session.Close(ctx)

	// 定义Cypher查询语句
	cqlCheck := "MATCH (a:exeNode {id: $id1})-[:to]->(b:exeNode {id: $id2}) RETURN count(*)"
	cqlCreate := "MATCH (a:exeNode {id: $id1}), (b:exeNode {id: $id2}) CREATE (a)-[:exeto{distance:" + distance + "}]->(b) RETURN 'Edge created successfully' AS message"
	// 检查是否已经存在关系
	result, err := session.Run(ctx, cqlCheck, map[string]interface{}{"id1": id1, "id2": id2})
	if err != nil {
		return "", err
	}

	if result.Next(ctx) {
		count, ok := result.Record().Get("count(*)")
		if !ok {
			return "", nil
		}
		if count.(int64) > 0 {
			return "Relationship already exists", nil
		}
	}

	// 执行查询创建关系
	result, err = session.Run(ctx, cqlCreate, map[string]interface{}{"id1": id1, "id2": id2})
	if err != nil {
		return "", err
	}

	// 检查结果中是否存在记录
	if result.Next(ctx) {
		return result.Record().Values[0].(string), nil
	}

	return "", nil
}

func (cn *cqlconnection) Addinfo(ctx context.Context, id string, name string, protocols string) (string, error) {
	// 创建Neo4j driver
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	// 创建Neo4j session
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})

	// 使用defer确保session关闭
	defer session.Close(ctx)

	// 定义Cypher查询语句
	// cqlUpdate := "MATCH (node:Node {id: $id}) SET node.name = $name, node.protocols = $protocols RETURN 'Node updated successfully' AS message"
	cqlUpdate := "MATCH (exe-Node) WHERE exeNode.id = $id SET exeNode.name = $name, exeNode.protocols = $protocols RETURN 'exeNode updated successfully' AS message"
	// 执行查询更新节点属性
	result, err := session.Run(ctx, cqlUpdate, map[string]interface{}{"id": id, "name": name, "protocols": protocols})
	if err != nil {
		return "", err
	}

	// 检查结果中是否存在记录
	if result.Next(ctx) {
		return result.Record().Values[0].(string), nil
	}

	return "", nil
}
