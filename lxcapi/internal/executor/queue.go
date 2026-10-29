package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"lxcapi/internal/core"
	"lxcapi/internal/db"
	"lxcapi/models"
	"lxcapi/pkg/logger"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type TaskFunc func(ctx context.Context) error

type Task struct {
	ID            uint
	ContainerName string
	Action        string
	Params        map[string]interface{}
	Func          TaskFunc
}

type Queue struct {
	tasks   chan *Task
	urgent  chan *Task
	workers int
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc

	// stateMu/closed 保护队列生命周期，避免 Stop 与 Submit 之间的竞态。
	stateMu sync.Mutex
	closed  bool

	// gates 按容器串行化任务执行：同一容器同一时间只有一个任务在跑，
	// 不同容器之间仍然由多个 Worker 并行。
	gatesMu sync.Mutex
	gates   map[string]*containerGate
}

// containerGate 是一个可取消的容器级互斥锁。
// refs 记录持有者与等待者数量，归零后从 map 中移除，避免无限增长。
type containerGate struct {
	sem  chan struct{}
	refs int
}

var GlobalQueue *Queue

func InitQueue() error {
	cfg := core.GlobalConfig.Task

	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 100
	}

	ctx, cancel := context.WithCancel(context.Background())

	GlobalQueue = &Queue{
		tasks:   make(chan *Task, cfg.QueueSize),
		urgent:  make(chan *Task, cfg.QueueSize),
		workers: cfg.Workers,
		ctx:     ctx,
		cancel:  cancel,
		gates:   make(map[string]*containerGate),
	}

	GlobalQueue.Start()
	logger.OK("异步任务队列初始化成功，Worker数: %d", cfg.Workers)
	return nil
}

func (q *Queue) Start() {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}
}

func (q *Queue) Stop() {
	if q == nil {
		return
	}

	q.stateMu.Lock()
	if q.closed {
		q.stateMu.Unlock()
		return
	}
	q.closed = true
	q.stateMu.Unlock()

	// 不关闭 tasks channel：Submit 可能正在发送，关闭会导致 send on closed。
	// Worker 通过 ctx 退出，等待中的删除任务也会因 ctx 取消而失败退出。
	q.cancel()
	q.wg.Wait()
	logger.Info("异步任务队列已停止")
}

// acquire 获取容器级执行锁，返回释放函数。
// ctx 取消时返回错误，避免请求断开或队列停止后永久阻塞。
func (q *Queue) acquire(ctx context.Context, containerName string) (func(), error) {
	q.gatesMu.Lock()
	gate := q.gates[containerName]
	if gate == nil {
		gate = &containerGate{sem: make(chan struct{}, 1)}
		q.gates[containerName] = gate
	}
	gate.refs++
	q.gatesMu.Unlock()

	releaseRef := func() {
		q.gatesMu.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(q.gates, containerName)
		}
		q.gatesMu.Unlock()
	}

	select {
	case gate.sem <- struct{}{}:
		return func() {
			<-gate.sem
			releaseRef()
		}, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}

func (q *Queue) Submit(task *Task) error {
	if q == nil {
		return fmt.Errorf("任务队列未启用")
	}

	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	if q.closed {
		return fmt.Errorf("任务队列已停止")
	}

	select {
	case q.tasks <- task:
		logger.Info("任务已提交: %s - %s", task.ContainerName, task.Action)
		return nil
	default:
		logger.Error("任务队列已满")
		return fmt.Errorf("任务队列已满")
	}
}

// SubmitDelete 提交删除任务。
// 删除任务进入独立的高优先级通道，由同一批 Worker 优先消费：
// 既不会排在其它容器的普通任务后面，并发数也受 task.workers 限制。
func (q *Queue) SubmitDelete(task *Task) error {
	if q == nil {
		return fmt.Errorf("任务队列未启用")
	}

	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	if q.closed {
		return fmt.Errorf("任务队列已停止")
	}

	select {
	case q.urgent <- task:
		logger.Info("删除任务已提交: %s", task.ContainerName)
		return nil
	default:
		logger.Error("删除任务队列已满")
		return fmt.Errorf("删除任务队列已满，请稍后重试")
	}
}

func (q *Queue) worker(id int) {
	defer q.wg.Done()
	
	logger.Info("Worker %d 已启动", id)
	
	for {
		// 优先处理删除任务，避免删除排在其它容器的普通任务后面。
		select {
		case <-q.ctx.Done():
			return
		case task := <-q.urgent:
			q.executeTask(task)
			continue
		default:
		}

		select {
		case <-q.ctx.Done():
			return
		case task := <-q.urgent:
			q.executeTask(task)
		case task := <-q.tasks:
			q.executeTask(task)
		}
	}
}

// executeTask 执行单个任务。
// 外层 defer recover 保证单个任务 panic 不会杀掉 Worker。
// 所有任务都通过容器级锁串行执行：删除任务会排在当前正在执行的任务之后，
// 等待其结束再删除；排队中的其它任务已在创建删除任务时被取消。
func (q *Queue) executeTask(task *Task) {
	defer func() {
		if r := recover(); r != nil {
			completed := time.Now()
			logger.Error("任务执行 panic: %s - %s: %v\n%s", task.ContainerName, task.Action, r, debug.Stack())
			db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
				"status":       models.TaskFailed,
				"error_msg":    fmt.Sprintf("任务异常中断: %v", r),
				"completed_at": &completed,
				"active_key":   nil,
			})
		}
	}()

	waitCtx := q.ctx
	if task.Action == "delete" {
		// 先写一条等待日志，用户在任务详情里能看到删除任务正在等待当前任务结束。
		waitingLog := fmt.Sprintf("[%s] 等待容器当前任务完成...", time.Now().Format("2006-01-02 15:04:05"))
		if err := db.DB.Model(&models.Task{}).
			Where("id = ? AND status = ?", task.ID, models.TaskQueued).
			Update("logs", waitingLog).Error; err != nil {
			logger.Warn("写入删除任务等待日志失败: %s - %s: %v", task.ContainerName, task.Action, err)
		}

		// 删除任务最多等待当前任务一个超时周期，避免任务永久停在 queued。
		var waitCancel context.CancelFunc
		waitCtx, waitCancel = context.WithTimeout(q.ctx, taskTimeout())
		defer waitCancel()
	}

	release, err := q.acquire(waitCtx, task.ContainerName)
	if err != nil {
		completed := time.Now()
		errorMsg := "任务队列已停止"
		if task.Action == "delete" && errors.Is(err, context.DeadlineExceeded) {
			errorMsg = "等待容器当前任务完成超时，请稍后重试"
		}
		logger.Warn("任务无法获取容器锁: %s - %s: %v", task.ContainerName, task.Action, err)
		db.DB.Model(&models.Task{}).Where("id = ? AND status = ?", task.ID, models.TaskQueued).
			Updates(map[string]interface{}{
				"status":       models.TaskFailed,
				"error_msg":    errorMsg,
				"completed_at": &completed,
				"active_key":   nil,
			})
		return
	}
	defer release()

	now := time.Now()

	var logs []string
	if task.Action == "delete" {
		logs = append(logs, "已等待容器当前任务结束")
	}
	logs = append(logs, fmt.Sprintf("[%s] 任务开始执行", now.Format("2006-01-02 15:04:05")))
	logs = append(logs, fmt.Sprintf("容器: %s, 操作: %s", task.ContainerName, task.Action))

	// 原子地 queued -> running：如果任务在排队期间已被删除操作取消，
	// 这里会命中 0 行，直接跳过执行，避免取消后仍然产生副作用。
	claim := db.DB.Model(&models.Task{}).
		Where("id = ? AND status = ?", task.ID, models.TaskQueued).
		Updates(map[string]interface{}{
			"status":     models.TaskRunning,
			"started_at": &now,
			"logs":       strings.Join(logs, "\n"),
		})
	if claim.Error != nil {
		logger.Error("任务状态更新失败: %s - %s: %v", task.ContainerName, task.Action, claim.Error)
		completed := time.Now()
		db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
			"status":       models.TaskFailed,
			"error_msg":    fmt.Sprintf("更新任务状态失败: %v", claim.Error),
			"completed_at": &completed,
			"active_key":   nil,
		})
		return
	}
	if claim.RowsAffected == 0 {
		logger.Info("任务已被取消或已完成，跳过执行: %s - %s", task.ContainerName, task.Action)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout())
	defer cancel()

	err = task.Func(ctx)

	completed := time.Now()
	duration := completed.Sub(now).Milliseconds()

	logs = append(logs, fmt.Sprintf("[%s] 任务执行完成", completed.Format("2006-01-02 15:04:05")))
	logs = append(logs, fmt.Sprintf("耗时: %dms", duration))

	if err != nil {
		logs = append(logs, fmt.Sprintf("执行失败: %v", err))

		db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
			"status":       models.TaskFailed,
			"error_msg":    err.Error(),
			"completed_at": &completed,
			"duration":     duration,
			"logs":         strings.Join(logs, "\n"),
			"active_key":   nil,
		})
		logger.Error("任务执行失败: %s - %s, 错误: %v", task.ContainerName, task.Action, err)
	} else {
		logs = append(logs, "执行成功")
		db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
			"status":       models.TaskSuccess,
			"result":       "操作执行成功",
			"completed_at": &completed,
			"duration":     duration,
			"logs":         strings.Join(logs, "\n"),
			"active_key":   nil,
		})
		logger.OK("任务执行成功: %s - %s", task.ContainerName, task.Action)
	}
}

func CreateTask(containerName, action string, params map[string]interface{}, fn TaskFunc) (*models.Task, error) {
	lockKey := activeLockKey(containerName, action)

	if action == "delete" {
		// 删除是容器生命周期的最后一步：排队中的任务还没有开始，直接取消，
		// 避免先白跑一遍重装/更新；正在执行的任务会在删除任务拿到容器锁后结束，
		// 然后删除任务才开始执行。
		if err := db.DB.Model(&models.Task{}).
			Where("container_name = ? AND status = ? AND action <> ?", containerName, models.TaskQueued, "delete").
			Updates(map[string]interface{}{
				"status":       models.TaskCancelled,
				"error_msg":    "已被删除任务取消",
				"completed_at": time.Now(),
				"active_key":   nil,
			}).Error; err != nil {
			return nil, fmt.Errorf("取消排队任务失败: %v", err)
		}
	} else {
		var count int64
		db.DB.Model(&models.Task{}).
			Where("container_name = ? AND status IN ?", containerName, []string{models.TaskQueued, models.TaskRunning}).
			Count(&count)

		if count > 0 {
			return nil, fmt.Errorf("容器正在执行任务，请稍后再试")
		}
	}

	var paramsJSON string
	if params != nil {
		data, _ := json.Marshal(params)
		paramsJSON = string(data)
	}

	task := &models.Task{
		ContainerName: containerName,
		Action:        action,
		Type:          "async",
		Status:        models.TaskQueued,
		Params:        paramsJSON,
		ActiveKey:     &lockKey,
	}

	if err := db.DB.Create(task).Error; err != nil {
		// 唯一索引兜底：并发提交时 Count 检查可能同时通过，由 active_key 拒绝重复插入。
		return nil, duplicateTaskError(err, lockKey)
	}

	queueTask := &Task{
		ID:            task.ID,
		ContainerName: containerName,
		Action:        action,
		Params:        params,
		Func:          fn,
	}

	var submitErr error
	if action == "delete" {
		// 删除任务绕过普通队列，直接等待该容器的容器锁。
		submitErr = GlobalQueue.SubmitDelete(queueTask)
	} else {
		submitErr = GlobalQueue.Submit(queueTask)
	}
	if submitErr != nil {
		db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
			"status":     models.TaskFailed,
			"error_msg":  fmt.Sprintf("提交任务队列失败: %v", submitErr),
			"active_key": nil,
		})
		return nil, submitErr
	}

	return task, nil
}

func CreateSyncTask(containerName, action string, fn TaskFunc) (err error) {
	lockKey := activeLockKey(containerName, action)

	var count int64
	db.DB.Model(&models.Task{}).
		Where("container_name = ? AND status IN ?", containerName, []string{models.TaskQueued, models.TaskRunning}).
		Count(&count)

	if count > 0 {
		return fmt.Errorf("容器正在执行任务，请稍后再试")
	}

	now := time.Now()

	task := &models.Task{
		ContainerName: containerName,
		Action:        action,
		Type:          "sync",
		Status:        models.TaskRunning,
		StartedAt:     &now,
		ActiveKey:     &lockKey,
	}

	if err := db.DB.Create(task).Error; err != nil {
		return duplicateTaskError(err, lockKey)
	}

	// 同步任务在请求线程内执行，panic 会抛给 gin Recovery，但任务行和 active_key
	// 不会自动清理。这里必须自行 recover，否则该容器后续所有任务都会被永久拒绝。
	defer func() {
		if r := recover(); r != nil {
			completed := time.Now()
			logger.Error("同步任务 panic: %s - %s: %v\n%s", containerName, action, r, debug.Stack())
			db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
				"status":       models.TaskFailed,
				"error_msg":    fmt.Sprintf("任务异常中断: %v", r),
				"completed_at": &completed,
				"active_key":   nil,
			})
			err = fmt.Errorf("任务异常中断: %v", r)
		}
	}()

	// 同步任务在请求线程内执行，同样必须获取容器级锁，
	// 否则可以和异步删除/重装并行修改同一个容器。
	var release func()
	if GlobalQueue != nil {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), taskTimeout())
		defer waitCancel()

		var acquireErr error
		release, acquireErr = GlobalQueue.acquire(waitCtx, containerName)
		if acquireErr != nil {
			completed := time.Now()
			db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
				"status":       models.TaskFailed,
				"error_msg":    "等待其他任务完成超时",
				"completed_at": &completed,
				"active_key":   nil,
			})
			return fmt.Errorf("容器正在执行其他任务，请稍后再试")
		}
		defer release()
	}

	var logs []string
	logs = append(logs, fmt.Sprintf("[%s] 任务开始执行", now.Format("2006-01-02 15:04:05")))
	logs = append(logs, fmt.Sprintf("容器: %s, 操作: %s", containerName, action))

	ctx, cancelFn := context.WithTimeout(context.Background(), taskTimeout())
	defer cancelFn()

	err = fn(ctx)

	completed := time.Now()
	duration := completed.Sub(now).Milliseconds()

	logs = append(logs, fmt.Sprintf("[%s] 任务执行完成", completed.Format("2006-01-02 15:04:05")))
	logs = append(logs, fmt.Sprintf("耗时: %dms", duration))

	if err != nil {
		logs = append(logs, fmt.Sprintf("执行失败: %v", err))

		db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
			"status":       models.TaskFailed,
			"error_msg":    err.Error(),
			"completed_at": &completed,
			"duration":     duration,
			"logs":         strings.Join(logs, "\n"),
			"active_key":   nil,
		})
		return err
	}

	db.DB.Model(&models.Task{}).Where("id = ?", task.ID).Updates(map[string]interface{}{
		"status":       models.TaskSuccess,
		"completed_at": &completed,
		"duration":     duration,
		"logs":         strings.Join(logs, "\n"),
		"active_key":   nil,
	})

	return nil
}

// activeLockKey 生成任务的并发去重键。
// 普通任务用容器名；删除任务用 "容器名:delete"，使删除任务可以在普通任务执行期间
// 创建并等待其结束，同时同一容器仍然不会出现两个删除任务。
func activeLockKey(containerName, action string) string {
	if action == "delete" {
		return containerName + ":delete"
	}
	return containerName
}

// hasActiveTask 判断去重键是否已被占用，用于把唯一索引冲突翻译成可读错误。
func hasActiveTask(lockKey string) bool {
	var count int64
	if err := db.DB.Model(&models.Task{}).Where("active_key = ?", lockKey).Count(&count).Error; err != nil {
		return false
	}
	return count > 0
}

// duplicateTaskError 把唯一索引冲突统一翻译为“容器正在执行任务”。
// 除错误文本匹配外再查一次 active_key，兼容各数据库不同的错误信息。
func duplicateTaskError(err error, lockKey string) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unique") || strings.Contains(msg, "duplicate") || strings.Contains(msg, "1062") {
		return fmt.Errorf("容器正在执行任务，请稍后再试")
	}
	if hasActiveTask(lockKey) {
		return fmt.Errorf("容器正在执行任务，请稍后再试")
	}
	return err
}

// taskTimeout 返回任务执行超时时间，配置为 0 或负数时回退到 10 分钟，
// 避免 context.WithTimeout 立即取消导致任务全部失败。
func taskTimeout() time.Duration {
	timeout := time.Duration(core.GlobalConfig.Task.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 600 * time.Second
	}
	return timeout
}

