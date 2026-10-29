package executor

import (
	"context"
	"database/sql"
	"lxcapi/internal/core"
	"lxcapi/internal/db"
	"lxcapi/models"
	"lxcapi/pkg/logger"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) {
	t.Helper()
	logger.Init("release", false)
	core.GlobalConfig = &core.Config{}
	sqlDB, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	// 任务执行 goroutine 与测试主协程会并发写库，SQLite 单连接避免 SQLITE_BUSY。
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { sqlDB.Close() })
	gdb, err := gorm.Open(sqlite.Dialector{Conn: sqlDB}, &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := gdb.AutoMigrate(&models.Task{}); err != nil {
		t.Fatal(err)
	}
	db.DB = gdb
}

func TestActiveKeyUniqueness(t *testing.T) {
	openTestDB(t)

	normal := "demo"
	if err := db.DB.Create(&models.Task{ContainerName: "demo", Action: "reinstall", Status: models.TaskQueued, ActiveKey: &normal}).Error; err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	dup := "demo"
	if err := db.DB.Create(&models.Task{ContainerName: "demo", Action: "update_config", Status: models.TaskQueued, ActiveKey: &dup}).Error; err == nil {
		t.Fatal("expected unique violation for duplicate active key")
	}

	// 删除任务使用独立键，应能与执行中的普通任务共存
	deleteKey := "demo:delete"
	if err := db.DB.Create(&models.Task{ContainerName: "demo", Action: "delete", Status: models.TaskQueued, ActiveKey: &deleteKey}).Error; err != nil {
		t.Fatalf("delete task should not collide: %v", err)
	}

	// 同为删除任务应被唯一索引拒绝
	deleteDup := "demo:delete"
	if err := db.DB.Create(&models.Task{ContainerName: "demo", Action: "delete", Status: models.TaskQueued, ActiveKey: &deleteDup}).Error; err == nil {
		t.Fatal("expected unique violation for duplicate delete task")
	}

	// 结束后置 NULL，允许多条历史记录
	if err := db.DB.Model(&models.Task{}).Where("action IN ?", []string{"reinstall", "delete"}).Update("active_key", nil).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Create(&models.Task{ContainerName: "a", Status: models.TaskSuccess}).Error; err != nil {
		t.Fatalf("multiple NULL active_key should be allowed: %v", err)
	}
	if err := db.DB.Create(&models.Task{ContainerName: "b", Status: models.TaskSuccess}).Error; err != nil {
		t.Fatalf("multiple NULL active_key should be allowed: %v", err)
	}

	reused := "demo"
	if err := db.DB.Create(&models.Task{ContainerName: "demo", Action: "create", Status: models.TaskQueued, ActiveKey: &reused}).Error; err != nil {
		t.Fatalf("active key should be reusable after NULL: %v", err)
	}
}

func TestGateSerializesPerContainer(t *testing.T) {
	q := &Queue{
		ctx:   context.Background(),
		gates: make(map[string]*containerGate),
	}

	release1, err := q.acquire(context.Background(), "c1")
	if err != nil {
		t.Fatal(err)
	}

	acquired := make(chan struct{})
	go func() {
		release2, err := q.acquire(context.Background(), "c1")
		if err != nil {
			t.Errorf("second acquire failed: %v", err)
			return
		}
		release2()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second acquire should block while first is held")
	case <-time.After(100 * time.Millisecond):
	}

	// 其他容器不应被阻塞
	releaseOther, err := q.acquire(context.Background(), "c2")
	if err != nil {
		t.Fatalf("different container should not block: %v", err)
	}
	releaseOther()

	release1()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("second acquire should succeed after release")
	}

	// 等待者全部退出后 gate 应从 map 中移除
	q.gatesMu.Lock()
	remaining := len(q.gates)
	q.gatesMu.Unlock()
	if remaining != 0 {
		t.Fatalf("expected gates map empty, got %d", remaining)
	}
}

func TestDeleteCancelsQueuedTasks(t *testing.T) {
	openTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		tasks:  make(chan *Task, 10),
		urgent: make(chan *Task, 10),
		ctx:    ctx,
		cancel: cancel,
		gates:  make(map[string]*containerGate),
	}
	GlobalQueue = q

	releaseDelete := make(chan struct{})
	defer func() {
		close(releaseDelete)
		q.Stop()
		GlobalQueue = nil
	}()

	name := "demo"
	queued := &models.Task{
		ContainerName: name,
		Action:        "reinstall",
		Status:        models.TaskQueued,
		ActiveKey:     &name,
	}
	if err := db.DB.Create(queued).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := CreateTask(name, "delete", nil, func(ctx context.Context) error {
		<-releaseDelete
		return nil
	}); err != nil {
		t.Fatalf("delete should be accepted while another task is queued: %v", err)
	}

	var cancelled models.Task
	if err := db.DB.First(&cancelled, queued.ID).Error; err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != models.TaskCancelled {
		t.Fatalf("queued task should be cancelled, got %s", cancelled.Status)
	}
	if cancelled.ActiveKey != nil {
		t.Fatal("cancelled task should release active_key")
	}

	// Worker 只能认领 queued 状态，取消后的任务不应再执行
	claim := db.DB.Model(&models.Task{}).
		Where("id = ? AND status = ?", queued.ID, models.TaskQueued).
		Updates(map[string]interface{}{"status": models.TaskRunning})
	if claim.RowsAffected != 0 {
		t.Fatal("cancelled task should not be claimable")
	}

	// 重复删除应被拒绝（删除任务仍在等待/执行，active_key 未释放）
	if _, err := CreateTask(name, "delete", nil, func(ctx context.Context) error { return nil }); err == nil {
		t.Fatal("duplicate delete task should be rejected")
	}

	// 删除任务排队期间，普通任务也不应再入队
	if _, err := CreateTask(name, "update_config", nil, func(ctx context.Context) error { return nil }); err == nil {
		t.Fatal("new task should be rejected while delete is queued")
	}
}

func TestDeleteWaitsForRunningTask(t *testing.T) {
	openTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		tasks:  make(chan *Task, 10),
		urgent: make(chan *Task, 10),
		ctx:    ctx,
		cancel: cancel,
		gates:  make(map[string]*containerGate),
	}
	q.workers = 1
	q.Start()
	t.Cleanup(q.Stop)

	// 模拟该容器上已有任务持有容器锁
	release, err := q.acquire(context.Background(), "demo")
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()

	key := "demo:delete"
	row := &models.Task{ContainerName: "demo", Action: "delete", Status: models.TaskQueued, ActiveKey: &key}
	if err := db.DB.Create(row).Error; err != nil {
		t.Fatal(err)
	}

	executed := make(chan struct{})
	task := &Task{
		ID:            row.ID,
		ContainerName: "demo",
		Action:        "delete",
		Func: func(ctx context.Context) error {
			close(executed)
			return nil
		},
	}

	if err := q.SubmitDelete(task); err != nil {
		t.Fatal(err)
	}

	// 容器锁未释放前，删除任务必须等待
	select {
	case <-executed:
		t.Fatal("delete task should wait for the running task")
	case <-time.After(300 * time.Millisecond):
	}

	var waiting models.Task
	if err := db.DB.First(&waiting, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if waiting.Status != models.TaskQueued {
		t.Fatalf("delete task should stay queued while waiting, got %s", waiting.Status)
	}
	if !strings.Contains(waiting.Logs, "等待容器当前任务完成") {
		t.Fatalf("delete task should write a waiting log, got %q", waiting.Logs)
	}

	release()
	released = true

	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("delete task should run after the running task finishes")
	}

	// 等待任务结果写库
	deadline := time.Now().Add(2 * time.Second)
	for {
		var finished models.Task
		if err := db.DB.First(&finished, row.ID).Error; err != nil {
			t.Fatal(err)
		}
		if finished.Status == models.TaskSuccess {
			if finished.ActiveKey != nil {
				t.Fatal("finished task should release active_key")
			}
			if !strings.Contains(finished.Logs, "已等待容器当前任务结束") {
				t.Fatalf("delete task should keep the wait log, got %q", finished.Logs)
			}
			break
		}
		if finished.Status == models.TaskFailed || finished.Status == models.TaskCancelled {
			t.Fatalf("delete task should succeed, got %s: %s", finished.Status, finished.ErrorMsg)
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete task did not finish, status=%s", finished.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSyncTaskPanicReleasesActiveKey(t *testing.T) {
	openTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		tasks:  make(chan *Task, 10),
		urgent: make(chan *Task, 10),
		ctx:    ctx,
		cancel: cancel,
		gates:  make(map[string]*containerGate),
	}
	GlobalQueue = q
	defer func() {
		q.Stop()
		GlobalQueue = nil
	}()

	err := CreateSyncTask("demo", "start", func(ctx context.Context) error {
		panic("boom")
	})
	if err == nil {
		t.Fatal("expected error from panicking sync task")
	}

	var task models.Task
	if err := db.DB.Where("container_name = ? AND action = ?", "demo", "start").
		First(&task).Error; err != nil {
		t.Fatal(err)
	}
	if task.Status != models.TaskFailed {
		t.Fatalf("task should be failed, got %s", task.Status)
	}
	if task.ActiveKey != nil {
		t.Fatal("panic should release active_key")
	}
	if task.CompletedAt == nil {
		t.Fatal("panic should set completed_at")
	}

	// 后续任务应能正常创建，证明容器没有被锁死
	if err := CreateSyncTask("demo", "stop", func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("subsequent task should be accepted: %v", err)
	}
}

func TestSubmitDeleteQueueFull(t *testing.T) {
	q := &Queue{
		tasks:  make(chan *Task, 1),
		urgent: make(chan *Task, 1),
		ctx:    context.Background(),
		gates:  make(map[string]*containerGate),
	}
	q.urgent <- &Task{}

	if err := q.SubmitDelete(&Task{}); err == nil {
		t.Fatal("expected delete queue full error")
	}
}

func TestGateCancel(t *testing.T) {
	q := &Queue{
		ctx:   context.Background(),
		gates: make(map[string]*containerGate),
	}
	release, err := q.acquire(context.Background(), "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := q.acquire(ctx, "c1"); err == nil {
		t.Fatal("expected acquire to fail on context timeout")
	}

	q.gatesMu.Lock()
	defer q.gatesMu.Unlock()
	if len(q.gates) != 1 {
		t.Fatalf("cancelled waiter should not leak gates, got %d", len(q.gates))
	}
}
