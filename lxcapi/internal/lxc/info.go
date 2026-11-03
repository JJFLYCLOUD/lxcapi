package lxc

import (
	"context"
	"fmt"
)

type ContainerInfo struct {
	Name         string                 `json:"name"`
	Status       string                 `json:"status"`
	Type         string                 `json:"type"`
	Architecture string                 `json:"architecture"`
	PID          int                    `json:"pid"`
	Created      string                 `json:"created_at"`
	LastUsed     string                 `json:"last_used_at"`
	Config       map[string]interface{} `json:"config"`
	Devices      map[string]interface{} `json:"devices"`
	State        *ContainerState        `json:"state"`
}

type ContainerState struct {
	Status     string                 `json:"status"`
	StatusCode int                    `json:"status_code"`
	Disk       map[string]interface{} `json:"disk"`
	Memory     map[string]interface{} `json:"memory"`
	Network    map[string]interface{} `json:"network"`
	Pid        int                    `json:"pid"`
	Processes  int                    `json:"processes"`
	CPU        map[string]interface{} `json:"cpu"`
}

// GetContainerConfig 只请求实例配置（1 次）。
func (c *Client) GetContainerConfig(ctx context.Context, name string) (*ContainerInfo, error) {
	var info ContainerInfo
	if err := c.get(ctx, fmt.Sprintf("/1.0/instances/%s", name), &info); err != nil {
		return nil, fmt.Errorf("获取容器配置失败: %v", err)
	}
	return &info, nil
}

// GetContainerState 只请求实例运行状态（1 次）。
func (c *Client) GetContainerState(ctx context.Context, name string) (*ContainerState, error) {
	var state ContainerState
	if err := c.get(ctx, fmt.Sprintf("/1.0/instances/%s/state", name), &state); err != nil {
		return nil, fmt.Errorf("获取容器状态失败: %v", err)
	}
	return &state, nil
}

// GetContainerInfo 请求实例配置 + 运行状态（2 次）。
func (c *Client) GetContainerInfo(ctx context.Context, name string) (*ContainerInfo, error) {
	info, err := c.GetContainerConfig(ctx, name)
	if err != nil {
		return nil, err
	}

	if state, err := c.GetContainerState(ctx, name); err == nil {
		info.State = state
	}

	return info, nil
}
