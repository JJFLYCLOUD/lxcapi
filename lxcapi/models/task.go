package models

import (
	"time"
)

const (
	TaskQueued    = "queued"
	TaskRunning   = "running"
	TaskSuccess   = "success"
	TaskFailed    = "failed"
	TaskCancelled = "cancelled"
)

type Task struct {
	ID            uint       `gorm:"primarykey" json:"ID"`
	CreatedAt     time.Time  `json:"CreatedAt"`
	UpdatedAt     time.Time  `json:"UpdatedAt"`
	ContainerName string     `gorm:"index;size:255" json:"container_name"`
	Action        string     `gorm:"size:100" json:"action"`
	Type          string     `gorm:"size:50" json:"type"`
	Status        string     `gorm:"index;size:50" json:"status"`
	Params        string     `gorm:"type:text" json:"params"`
	Result        string     `gorm:"type:text" json:"result"`
	Logs          string     `gorm:"type:text" json:"logs"`
	ErrorMsg      string     `gorm:"type:text" json:"error_msg"`
	StartedAt     *time.Time `json:"started_at"`
	CompletedAt   *time.Time `json:"completed_at"`
	Duration      int64      `json:"duration"`

	// ActiveKey 是并发去重键：同一容器同一时间只允许一个普通任务，
	// 删除任务单独占一个键（"容器名:delete"），以便在已有任务执行时排队等待。
	// 任务结束后置为 NULL；数据库唯一索引保证并发提交不会插入重复任务。
	ActiveKey *string `gorm:"uniqueIndex;size:255" json:"-"`
}

