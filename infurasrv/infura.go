package infurasrv

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// 结构体用于封装JSON RPC请求和响应
type jsonRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type jsonRPCResponse struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      int                    `json:"id"`
	Result  map[string]interface{} `json:"result"`
}

// 函数用于通过Infura获取区块高度
func GetBlockNumberByHash(blockHash string) (string, error) {

	infuraProjectID := "9bc799bb0d97435696cb99d8ad586245"
	// 构造请求体
	requestBody := jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  "eth_getBlockByHash",
		Params:  []interface{}{blockHash, false},
		ID:      1,
	}
	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		return "", err
	}

	// 发送请求到Infura
	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", infuraProjectID)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBytes))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 解析响应体
	responseBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var response jsonRPCResponse
	err = json.Unmarshal(responseBytes, &response)
	if err != nil {
		return "", err
	}

	// 从响应中提取区块高度
	blockNumber, ok := response.Result["number"].(string)
	if !ok {
		return "", fmt.Errorf("block number not found in response")
	}

	return blockNumber, nil
}