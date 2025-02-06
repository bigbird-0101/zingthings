package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strings"
	"zingthings/pkg/protocol/core"
)

type (
	Client struct {
		logger     *zap.Logger
		url        string
		httpClient *http.Client
	}
)

func MakeClient(logger *zap.Logger, containerUrl string) *Client {
	hc := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	return &Client{
		logger:     logger.Named("builder_client"),
		url:        strings.TrimSuffix(containerUrl, "/"),
		httpClient: hc,
	}
}

func (client *Client) Deploy(req *core.DeployRequest) (*core.Response, error) {
	marshal, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := client.httpClient.Post(client.url+"/deploy", "application/json", bytes.NewReader(marshal))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status code: %d", resp.StatusCode)
	}
	body := resp.Body
	all, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	response := &core.Response{}
	err = json.Unmarshal(all, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (client *Client) UnDeploy(req *core.UnDeployRequest) (*core.Response, error) {
	marshal, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := client.httpClient.Post(client.url+"/undeploy", "application/json", bytes.NewReader(marshal))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status code: %d", resp.StatusCode)
	}
	body := resp.Body
	all, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	response := &core.Response{}
	err = json.Unmarshal(all, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (client *Client) GetProtocolStatus(protocolInfo *core.ProtocolInfo) (core.Status, error) {
	marshal, err := json.Marshal(protocolInfo)
	if err != nil {
		return "", err
	}
	resp, err := client.httpClient.Post(client.url+"/protocolStatus", "application/json", bytes.NewReader(marshal))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http status code: %d", resp.StatusCode)
	}
	body := resp.Body
	all, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	response := &core.Response{}
	err = json.Unmarshal(all, response)
	if err != nil {
		return "", err
	}
	bytesStatus, err := json.Marshal(response.Data)
	status := &core.ProtocolStatus{}
	err = json.Unmarshal(bytesStatus, status)
	if err != nil {
		return "", err
	}
	return status.Status, nil
}
