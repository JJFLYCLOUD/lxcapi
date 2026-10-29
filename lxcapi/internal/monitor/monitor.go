package monitor

import (
	"context"
	"fmt"
	"lxcapi/internal/core"
	"lxcapi/internal/db"
	"lxcapi/internal/ipv6"
	"lxcapi/internal/lxc"
	"lxcapi/models"
	"lxcapi/pkg/logger"
	"lxcapi/pkg/plugin"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
)

type Monitor struct {
	lxcClient *lxc.Client
	interval  time.Duration
	neighbor  atomic.Value

	lastResetMonth   int
	lastResetDay     int
	lastResetAttempt time.Time

	proxyMu      sync.Mutex
	cycleDesired map[string]struct{}
	proxyMissing map[string]int
}

// proxyNDPStaleGrace 为一个代理NDP条目连续缺席多少个监控周期后才被清理，
// 用于规避单次采集抖动导致的短暂断网。
const proxyNDPStaleGrace = 2

// resetRetryInterval 自动重置流量失败后的重试间隔。
// 成功完成一次全量重置后按天/月标记跳过；失败时按该间隔重试，
// 而不是像以前一样等到下一个天/月周期。
const resetRetryInterval = time.Minute

var GlobalMonitor *Monitor

func InitMonitor() error {
	cfg := core.GlobalConfig.Traffic

	GlobalMonitor = &Monitor{
		lxcClient: lxc.DefaultClient(),
		interval:  time.Duration(cfg.Interval) * time.Second,
	}

	GlobalMonitor.reloadNeighborConfig()

	logger.OK("流量监控器初始化成功")
	return nil
}

func (m *Monitor) Start(ctx context.Context) {
	if m == nil {
		return
	}

	logger.Info("流量监控已启动，采样间隔: %v", m.interval)

	// 启动时全量补重置一次未重置流量
	m.maybeAutoResetTraffic()

	for {
		rows := m.loadRows()

		m.beginProxyCycle()

		for i := range rows {
			select {
			case <-ctx.Done():
				return
			default:
			}

			m.process(&rows[i])

			if i < len(rows)-1 {
				if !m.wait(ctx, m.interval) {
					return
				}
			}
		}

		// 本周期结束后对账：补齐缺失的代理NDP条目，清理连续缺席的残留条目
		m.reconcileProxyNDP()

		// 仅跨月时才重新查询并重置
		m.maybeAutoResetTraffic()

		if !m.wait(ctx, m.interval) {
			return
		}
	}
}

// wait 等待指定时长，期间可被 ctx 中断，返回 false 表示已取消。
func (m *Monitor) wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (m *Monitor) loadRows() []models.Container {
	var containers []models.Container
	if err := db.DB.Select("name", "status", "traffic_limit", "traffic_usage", "rx_bytes", "tx_bytes", "traffic_over", "traffic_over_action", "traffic_over_limitspeed", "ingress", "egress").
		Find(&containers).Error; err != nil {
		logger.Error("获取容器列表失败: %v", err)
		return nil
	}
	return containers
}

// process 按预定义三分支处理单个容器。
func (m *Monitor) process(c *models.Container) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state, err := m.getContainerState(ctx, c.Name)
	if err != nil {
		return
	}

	// 先对账带宽：处理“超限标记仍在、但带宽恢复曾失败”的容器
	m.reconcileBandwidth(c, state)

	switch state.Status {
	case "Running":
		switch c.Status {
		case "frozen":
			m.stopFrozenContainer(c.Name)
		case "running", "stopped", "limited":
			if c.Status == "stopped" {
				if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Update("status", "running").Error; err != nil {
					logger.Error("更新容器状态失败 %s: %v", c.Name, err)
				}
			}
			m.collectRunning(c, state)
		}
	case "Stopped":
		if c.Status == "running" {
			m.restartContainer(c.Name)
		}
	}
}

// stopFrozenContainer DB frozen 但实际 running，强制停止保持 frozen。
func (m *Monitor) stopFrozenContainer(name string) {
	logger.Warn("检测到数据库frozen但实际running，强制停止容器: %s", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.lxcClient.StopContainer(ctx, name); err != nil {
		logger.Warn("强制停止容器失败 %s: %v", name, err)
	}
}

// restartContainer DB running 但实际 stopped，重新启动。
func (m *Monitor) restartContainer(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.lxcClient.StartContainer(ctx, name); err != nil {
		logger.Warn("检测到容器意外停止后自动重启失败 %s: %v", name, err)
		return
	}
	db.DB.Model(&models.Container{}).Where("name = ?", name).Update("status", "running")
	logger.OK("检测到容器意外停止，已自动重启: %s", name)
}

// collectRunning DB running 且实际 running：计算流量增量写库，超限则冻结，否则采集内存硬盘。
func (m *Monitor) collectRunning(c *models.Container, state *containerUsageSnapshot) {
	newRx := state.RxBytes
	newTx := state.TxBytes

	deltaRx := trafficDelta(newRx, uint64(c.RxBytes))
	deltaTx := trafficDelta(newTx, uint64(c.TxBytes))
	incrementGB := float64(deltaRx+deltaTx) / (1024 * 1024 * 1024)

	totalGB := c.TrafficUsage + incrementGB

	updates := map[string]interface{}{
		"rx_bytes":      int64(newRx),
		"tx_bytes":      int64(newTx),
		"traffic_usage": totalGB,
	}

	if c.TrafficLimit > 0 && totalGB >= float64(c.TrafficLimit) {
		action := strings.ToLower(strings.TrimSpace(c.TrafficOverAction))
		if action == "" {
			action = "frozen"
		}

		if action == "limited" {
			// 已经成功限速过，后续周期只需继续记账
			if c.Status == "limited" {
				updates["traffic_over"] = true
				if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Updates(updates).Error; err != nil {
					logger.Error("更新容器流量数据失败 %s: %v", c.Name, err)
				}
				return
			}

			// 先真正下发限速，成功后再写状态；失败只记账，下个周期重试，
			// 避免出现“数据库显示已限速、实际仍全速”的假状态。
			if err := m.handleLimited(c, totalGB); err != nil {
				logger.Error("容器 %s 限速失败，将在下个监控周期重试: %v", c.Name, err)
				if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Updates(updates).Error; err != nil {
					logger.Error("更新容器流量数据失败 %s: %v", c.Name, err)
				}
				return
			}

			updates["traffic_over"] = true
			updates["status"] = "limited"
			if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Updates(updates).Error; err != nil {
				logger.Error("更新容器限速状态失败 %s: %v", c.Name, err)
			}
			logger.Warn("容器 %s 流量超限: %.4fGB / %dGB，已触发限速", c.Name, totalGB, c.TrafficLimit)
			return
		}

		updates["traffic_over"] = true
		updates["status"] = "frozen"
		if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Updates(updates).Error; err != nil {
			// 状态没写进数据库时不能停机，否则下一周期会看到 DB running + 实际 stopped，
			// 被监控误判为意外停止并自动拉起，形成“停止—重启”循环。
			logger.Error("冻结容器状态写库失败 %s: %v，跳过本次停机", c.Name, err)
			return
		}

		logger.Warn("容器 %s 流量超限: %.4fGB / %dGB", c.Name, totalGB, c.TrafficLimit)
		m.handleOverLimit(c.Name, totalGB, c.TrafficLimit)
		return
	}

	updates["memory_usage_raw"] = uint64(state.MemUsage)
	updates["disk_usage_raw"] = uint64(state.DiskUsage)
	updates["last_sync"] = time.Now()
	if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Updates(updates).Error; err != nil {
		logger.Error("更新容器监控数据失败 %s: %v", c.Name, err)
	}

	m.handleIPv6NeighborRequests(c.Name, state)
}

func trafficDelta(cur, base uint64) uint64 {
	if cur >= base {
		return cur - base
	}
	return cur
}

func (m *Monitor) handleIPv6NeighborRequests(containerName string, state *containerUsageSnapshot) {
	if len(state.IPv6s) == 0 {
		return
	}

	cfg := m.neighbor.Load().(models.IPv6NeighborConfig)
	if !cfg.Enabled || cfg.Iface == "" || cfg.Gateway == "" || cfg.Prefix == "" {
		return
	}

	seen := make(map[string]bool)
	for _, ip := range state.IPv6s {
		if !ipv6.InPrefix(ip, cfg.Prefix) || seen[ip] {
			continue
		}
		seen[ip] = true

		m.markDesired(ip)

		if err := ipv6.AnnounceNeighbor(cfg.Iface, ip, cfg.Gateway); err != nil {
			logger.Warn("容器 %s IPv6邻居请求发送失败 %s -> %s: %v", containerName, ip, cfg.Gateway, err)
			continue
		}
	}
}

func (m *Monitor) beginProxyCycle() {
	m.proxyMu.Lock()
	m.cycleDesired = make(map[string]struct{})
	m.proxyMu.Unlock()
}

func (m *Monitor) markDesired(addr string) {
	m.proxyMu.Lock()
	if m.cycleDesired != nil {
		m.cycleDesired[addr] = struct{}{}
	}
	m.proxyMu.Unlock()
}

// reconcileProxyNDP 在每个监控周期结束时对账内核代理NDP条目：
//   - 主开关或代理NDP开关关闭时，清理本前缀的全部条目；
//   - 否则补齐“应存在但内核缺失”的条目；
//   - 对“本前缀内、且连续缺席达到 proxyNDPStaleGrace 个周期”的条目执行清理，
//     前缀外的条目一律不触碰。
func (m *Monitor) reconcileProxyNDP() {
	cfg := m.neighbor.Load().(models.IPv6NeighborConfig)

	if !cfg.Enabled {
		m.resetProxyState()
		if cfg.Iface != "" && cfg.Prefix != "" {
			if err := ipv6.FlushProxyNDP(cfg.Iface, cfg.Prefix); err != nil {
				logger.Warn("清理IPv6代理NDP条目失败 dev %s: %v", cfg.Iface, err)
			}
		}
		return
	}

	if cfg.Iface == "" || cfg.Gateway == "" || cfg.Prefix == "" {
		return
	}

	m.proxyMu.Lock()
	desired := make(map[string]struct{}, len(m.cycleDesired))
	for k := range m.cycleDesired {
		desired[k] = struct{}{}
	}
	m.proxyMu.Unlock()

	cur, err := ipv6.ListProxyNDP(cfg.Iface)
	if err != nil {
		logger.Warn("查询IPv6代理NDP条目失败 dev %s: %v", cfg.Iface, err)
		return
	}

	current := make(map[string]struct{}, len(cur))
	for _, ip := range cur {
		current[ip.String()] = struct{}{}
	}

	m.proxyMu.Lock()
	if m.proxyMissing == nil {
		m.proxyMissing = make(map[string]int)
	}

	// 方向A：清理残留（仅限本前缀，连续缺席达到宽限才删）
	for key := range current {
		if !ipv6.InPrefix(key, cfg.Prefix) {
			continue
		}
		if _, ok := desired[key]; ok {
			delete(m.proxyMissing, key)
			continue
		}
		m.proxyMissing[key]++
		if m.proxyMissing[key] >= proxyNDPStaleGrace {
			if err := ipv6.DelProxyNDP(cfg.Iface, key); err != nil {
				logger.Warn("清理残留代理NDP条目失败 %s dev %s: %v", key, cfg.Iface, err)
			} else {
				logger.Info("已清理残留IPv6代理NDP条目 %s dev %s", key, cfg.Iface)
			}
			delete(m.proxyMissing, key)
		}
	}

	// 清理计数中已不在内核里的地址
	for key := range m.proxyMissing {
		if _, ok := current[key]; !ok {
			delete(m.proxyMissing, key)
		}
	}
	m.proxyMu.Unlock()

	// 方向B：补齐应存在但内核缺失的条目
	for key := range desired {
		if _, ok := current[key]; ok {
			continue
		}
		if err := ipv6.AddProxyNDP(cfg.Iface, key); err != nil {
			logger.Warn("补齐IPv6代理NDP条目失败 %s dev %s: %v", key, cfg.Iface, err)
		}
	}
}

func (m *Monitor) resetProxyState() {
	m.proxyMu.Lock()
	m.proxyMissing = nil
	m.proxyMu.Unlock()
}

// refreshProxyNDP 供配置保存后立即生效：查询当前运行中的容器并补发邻居请求、
// 添加代理NDP条目，无需等待下一个监控周期。
func (m *Monitor) refreshProxyNDP() {
	cfg := m.neighbor.Load().(models.IPv6NeighborConfig)
	if !cfg.Enabled || cfg.Iface == "" || cfg.Gateway == "" || cfg.Prefix == "" {
		return
	}

	for _, c := range m.loadRows() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := m.getContainerState(ctx, c.Name)
		cancel()
		if err != nil || state.Status != "Running" {
			continue
		}
		m.handleIPv6NeighborRequests(c.Name, state)
	}
}

func (m *Monitor) reloadNeighborConfig() {
	cfg, err := ipv6.GetNeighborConfig()
	if err != nil {
		cfg = models.IPv6NeighborConfig{}
	}
	m.neighbor.Store(cfg)
}

func ReloadNeighborConfig() {
	if GlobalMonitor != nil {
		GlobalMonitor.reloadNeighborConfig()
	}
}

// RefreshProxyNDP 在配置保存后立即对运行中容器生效（补发邻居请求、添加代理NDP条目）。
func RefreshProxyNDP() {
	if GlobalMonitor != nil {
		GlobalMonitor.refreshProxyNDP()
	}
}

type containerUsageSnapshot struct {
	Status    string
	MemUsage  float64
	DiskUsage float64
	RxBytes   uint64
	TxBytes   uint64
	IPv6s     []string
}

// getContainerState 使用 gjson 单次扫描（GetManyBytes）提取 LXD state 响应字段，
// 避免多次 Get 对 metadata 子串的重复词法解析。
func (m *Monitor) getContainerState(ctx context.Context, name string) (*containerUsageSnapshot, error) {
	data, err := m.lxcClient.GetRaw(ctx, fmt.Sprintf("/1.0/instances/%s/state", name))
	if err != nil {
		return nil, err
	}

	res := gjson.GetManyBytes(data,
		"metadata.status",
		"metadata.memory.usage",
		"metadata.disk.root.usage",
		"metadata.network.eth0.counters.bytes_received",
		"metadata.network.eth0.counters.bytes_sent",
		"metadata.network.eth0.addresses",
		"metadata.network.eth1.addresses",
	)

	snap := &containerUsageSnapshot{Status: res[0].String()}

	snap.MemUsage = res[1].Float()

	if res[2].Exists() {
		snap.DiskUsage = res[2].Float()
	}

	snap.RxBytes = res[3].Uint()
	snap.TxBytes = res[4].Uint()

	// 采集 eth0/eth1 网卡；addresses 存在才解析过滤 inet6
	for _, i := range []int{5, 6} {
		if addrs := res[i]; addrs.Exists() {
			addrs.ForEach(func(_, v gjson.Result) bool {
				if v.Get("family").String() == "inet6" {
					addr := v.Get("address").String()
					if addr != "" && !strings.HasPrefix(addr, "fe80:") {
						snap.IPv6s = append(snap.IPv6s, addr)
					}
				}
				return true
			})
		}
	}

	return snap, nil
}

func (m *Monitor) handleLimited(c *models.Container, currentGB float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	speed := c.TrafficOverLimitSpeed
	if speed <= 0 {
		speed = 1
	}

	if err := m.lxcClient.SetContainerBandwidth(ctx, c.Name, speed, speed); err != nil {
		return fmt.Errorf("设置限速失败: %v", err)
	}

	logger.OK("容器流量超限已限速至 %dMbit: %s", speed, c.Name)

	if mgr := plugin.GetManager(); mgr != nil {
		mgr.GetHookManager().TriggerAsync(plugin.HookTrafficOverLimit, ctx, map[string]interface{}{
			"name":    c.Name,
			"current": uint64(currentGB * 1024 * 1024 * 1024),
			"limit":   uint64(c.TrafficLimit) * 1024 * 1024 * 1024,
			"action":  "limited",
			"speed":   speed,
		})
	}

	return nil
}

// restoreSpeed 恢复容器原始入站/出站带宽（流量重置或解除限速时调用）
// 原始值为 0 表示该方向不限速，由 SetContainerBandwidth 删除对应的 limits 键。
func (m *Monitor) restoreSpeed(container *models.Container) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := m.lxcClient.SetContainerBandwidth(ctx, container.Name, container.Ingress, container.Egress); err != nil {
		return fmt.Errorf("恢复容器带宽失败: %v", err)
	}
	logger.OK("已恢复容器原始带宽: %s", container.Name)
	return nil
}

// needsBandwidthRestore 判断重置流量前是否需要把带宽恢复回数据库记录值。
// frozen 容器可能残留限速配置（例如限速后被手动暂停），也需要恢复。
func needsBandwidthRestore(c *models.Container) bool {
	return c.Status == "limited" || c.Status == "frozen" || c.TrafficOver
}

// reconcileBandwidth 对“已经不再超限、但带宽恢复失败”的容器重试恢复带宽，
// 避免一次 Incus 抖动导致容器永久停留在限速值。
func (m *Monitor) reconcileBandwidth(c *models.Container, state *containerUsageSnapshot) {
	if !c.TrafficOver {
		return
	}
	if c.TrafficLimit > 0 && c.TrafficUsage >= float64(c.TrafficLimit) {
		// 仍在超限状态，限速是预期行为
		return
	}

	if err := m.restoreSpeed(c); err != nil {
		logger.Warn("重试恢复容器带宽失败 %s: %v", c.Name, err)
		return
	}

	updates := map[string]interface{}{
		"traffic_over": false,
	}

	// frozen 容器只恢复带宽，不改变暂停状态；是否是超限冻结由 traffic_over 判断。
	if c.Status != "frozen" {
		status := "stopped"
		if state != nil && state.Status == "Running" {
			status = "running"
		}
		updates["status"] = status
	}

	if err := db.DB.Model(&models.Container{}).Where("name = ?", c.Name).Updates(updates).Error; err != nil {
		logger.Error("更新容器带宽恢复状态失败 %s: %v", c.Name, err)
		return
	}

	logger.OK("容器带宽已自动对账恢复: %s", c.Name)
}

func (m *Monitor) handleOverLimit(containerName string, current float64, limit int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.lxcClient.StopContainer(ctx, containerName); err != nil {
		logger.Error("自动停止超限容器失败 %s: %v", containerName, err)
		return
	}

	logger.OK("容器因流量超限已锁定并停止: %s", containerName)

	if mgr := plugin.GetManager(); mgr != nil {
		mgr.GetHookManager().TriggerAsync(plugin.HookTrafficOverLimit, ctx, map[string]interface{}{
			"name":    containerName,
			"current": uint64(current * 1024 * 1024 * 1024),
			"limit":   uint64(limit) * 1024 * 1024 * 1024,
		})
	}
}

type TrafficInfo struct {
	ContainerName string    `json:"container_name"`
	RxBytes       int64     `json:"rx_bytes"`
	TxBytes       int64     `json:"tx_bytes"`
	TrafficUsage  float64   `json:"traffic_usage"`
	TrafficLimit  int       `json:"traffic_limit"`
	TrafficResetCycle string `json:"traffic_reset_cycle"`
	TrafficOverAction string `json:"traffic_over_action"`
	TrafficOverLimitSpeed int `json:"traffic_over_limitspeed"`
	TrafficOver   bool      `json:"traffic_over"`
	LastReset     time.Time `json:"last_reset"`
}

func (m *Monitor) GetTraffic(containerName string) (*TrafficInfo, error) {
	var container models.Container
	if err := db.DB.Where("name = ?", containerName).
		Select("name", "traffic_usage", "traffic_limit", "rx_bytes", "tx_bytes", "traffic_over", "last_reset",
			"traffic_reset_cycle", "traffic_over_action", "traffic_over_limitspeed").
		First(&container).Error; err != nil {
		return nil, err
	}
	return &TrafficInfo{
		ContainerName: container.Name,
		RxBytes:       container.RxBytes,
		TxBytes:       container.TxBytes,
		TrafficUsage:  container.TrafficUsage,
		TrafficLimit:  container.TrafficLimit,
		TrafficResetCycle: container.TrafficResetCycle,
		TrafficOverAction: container.TrafficOverAction,
		TrafficOverLimitSpeed: container.TrafficOverLimitSpeed,
		TrafficOver:   container.TrafficOver,
		LastReset:     container.LastReset,
	}, nil
}

// maybeAutoResetTraffic 控制自动重置的执行时机：
//   - 当天/当月已经成功完成过一次全量重置时直接返回；
//   - 上一轮存在失败时，按 resetRetryInterval 重试，而不是等到下一个周期。
func (m *Monitor) maybeAutoResetTraffic() {
	now := time.Now()
	curMonth := now.Year()*100 + int(now.Month())
	curDay := curMonth*100 + now.Day()
	if m.lastResetMonth == curMonth && m.lastResetDay == curDay {
		return
	}

	// 上一轮失败过：按重试间隔退避，避免每个监控周期都查询全表
	if !m.lastResetAttempt.IsZero() && now.Sub(m.lastResetAttempt) < resetRetryInterval {
		return
	}

	m.lastResetAttempt = now
	if m.autoResetTraffic() {
		m.lastResetMonth = curMonth
		m.lastResetDay = curDay
	}
}

// autoResetTraffic 执行一轮自动重置，返回是否全部成功。
// 只要存在失败（查询、取状态、写库），调用方就不会推进天/月标记，
// 下一轮会重新扫描，已成功的容器因 last_reset 已更新会被跳过。
func (m *Monitor) autoResetTraffic() bool {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	firstDayOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())

	var containers []models.Container
	// 只查询本周期需要重置的行：
	// - daily: last_reset 早于今天
	// - 其他（含空字符串）: last_reset 早于本月1号
	if err := db.DB.Select("name", "status", "traffic_over", "last_reset", "traffic_reset_cycle", "traffic_over_action", "ingress", "egress").
		Where("traffic_reset_cycle = ? AND (last_reset IS NULL OR last_reset < ?)", "daily", today).
		Or("(traffic_reset_cycle IS NULL OR traffic_reset_cycle <> ?) AND (last_reset IS NULL OR last_reset < ?)", "daily", firstDayOfMonth).
		Find(&containers).Error; err != nil {
		logger.Error("查询待重置流量记录失败: %v", err)
		return false
	}

	failures := 0

	for _, container := range containers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, err := m.getContainerState(ctx, container.Name)
		cancel()

		if err != nil {
			logger.Error("重置容器流量时获取网卡状态失败 %s: %v", container.Name, err)
			failures++
			continue
		}

		updates := map[string]interface{}{
			"rx_bytes":      int64(state.RxBytes),
			"tx_bytes":      int64(state.TxBytes),
			"traffic_usage": 0.0,
			"last_reset":    now,
		}

		// frozen 容器可能残留限速配置（例如限速后被手动暂停），因此也需要恢复带宽。
		// 这里采用 best-effort：失败只告警，不阻塞统计重置。
		if container.Status == "frozen" {
			if err := m.restoreSpeed(&container); err != nil {
				// 恢复失败时保留 traffic_over，交给 reconcileBandwidth 继续重试，
				// 避免带宽永久停留在限速值。
				logger.Warn("冻结容器恢复带宽失败 %s: %v，保留标记等待重试", container.Name, err)
				updates["traffic_over"] = container.TrafficOver
			} else {
				updates["traffic_over"] = false
			}

			// 超限冻结的容器重置后自动开机；手动暂停的保持 frozen 不开机。
			if container.TrafficOver {
				runCtx, runCancel := context.WithTimeout(context.Background(), 10*time.Second)
				startErr := m.lxcClient.StartContainer(runCtx, container.Name)
				runCancel()

				switch {
				case startErr == nil:
					updates["status"] = "running"
					logger.OK("流量重置后已自动开机: %s", container.Name)
				case strings.Contains(startErr.Error(), "already running"):
					updates["status"] = "running"
					logger.Info("容器已在运行中，跳过自动开机: %s", container.Name)
				default:
					updates["status"] = "stopped"
					logger.Error("流量重置后自动开机失败 %s: %v", container.Name, startErr)
				}
			}

			if err := db.DB.Model(&models.Container{}).Where("name = ?", container.Name).Updates(updates).Error; err != nil {
				logger.Error("重置容器流量失败 %s: %v", container.Name, err)
				failures++
				continue
			}
			logger.OK("容器流量已自动重置: %s", container.Name)
			continue
		}

		// 非 frozen：需要恢复带宽时先执行 Incus 操作，成功后才清除限速标记。
		// 否则会出现“数据库显示已恢复、容器实际仍被限速”的假成功，
		// 且 status 已变成 running 后再也不会触发 restoreSpeed。
		if needsBandwidthRestore(&container) {
			if err := m.restoreSpeed(&container); err != nil {
				logger.Error("容器 %s 流量已清零但带宽恢复失败，保留限速状态等待自动重试: %v", container.Name, err)
				if err := db.DB.Model(&models.Container{}).Where("name = ?", container.Name).Updates(updates).Error; err != nil {
					logger.Error("重置容器流量失败 %s: %v", container.Name, err)
					failures++
				}
				continue
			}
		}

		updates["traffic_over"] = false
		if container.Status == "limited" {
			updates["status"] = "running"
		}

		if err := db.DB.Model(&models.Container{}).Where("name = ?", container.Name).Updates(updates).Error; err != nil {
			logger.Error("重置容器流量失败 %s: %v", container.Name, err)
			failures++
			continue
		}

		logger.OK("容器流量已自动重置: %s", container.Name)
	}

	if failures > 0 {
		logger.Warn("自动重置流量有 %d 个容器失败，将在 %v 后重试", failures, resetRetryInterval)
		return false
	}

	return true
}

func (m *Monitor) ResetTraffic(containerName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	state, err := m.getContainerState(ctx, containerName)
	cancel()
	if err != nil {
		return fmt.Errorf("获取容器流量数据失败")
	}

	var container models.Container
	if err := db.DB.Select("name", "status", "traffic_limit", "traffic_reset_cycle", "traffic_over", "ingress", "egress").
		Where("name = ?", containerName).First(&container).Error; err != nil {
		return fmt.Errorf("容器不存在: %v", err)
	}

	// 带宽恢复失败时直接返回错误并保留限速标记，避免数据库显示成功、容器仍被限速。
	if needsBandwidthRestore(&container) {
		if err := m.restoreSpeed(&container); err != nil {
			return fmt.Errorf("恢复容器带宽失败，流量未重置: %v", err)
		}
	}

	err = db.DB.Model(&models.Container{}).Where("name = ?", containerName).Updates(map[string]interface{}{
		"rx_bytes":      int64(state.RxBytes),
		"tx_bytes":      int64(state.TxBytes),
		"traffic_usage": 0.0,
		"traffic_over":  false,
		"last_reset":    time.Now(),
	}).Error

	if err != nil {
		return fmt.Errorf("重置流量统计失败: %v", err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.lxcClient.StartContainer(runCtx, containerName); err != nil {
		if strings.Contains(err.Error(), "already running") {
			db.DB.Model(&models.Container{}).Where("name = ?", containerName).Update("status", "running")
			logger.Info("容器已在运行中，跳过自动开机: %s", containerName)
		} else {
			db.DB.Model(&models.Container{}).Where("name = ?", containerName).Update("status", "stopped")
			logger.Error("重置流量后自动开机失败 %s: %v", containerName, err)
		}
	} else {
		db.DB.Model(&models.Container{}).Where("name = ?", containerName).Update("status", "running")
		logger.OK("重置流量后已自动开机: %s", containerName)
	}

	cycle := "月度"
	if strings.EqualFold(strings.TrimSpace(container.TrafficResetCycle), "daily") {
		cycle = "每日"
	}

	logger.OK("流量统计已重置: %s (周期: %s, 时间: %s)",
		containerName,
		cycle,
		time.Now().Format("2006-01-02 15:04:05"))
	return nil
}