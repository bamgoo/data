package data

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
)

func init() {
	host = bamgoo.Mount(module)
	registerCacheSyncService(host)
}

var module = &Module{
	configs:     make(Configs, 0),
	drivers:     make(map[string]Driver, 0),
	instances:   make(map[string]*Instance, 0),
	tables:      make(map[string]Table, 0),
	views:       make(map[string]View, 0),
	models:      make(map[string]Model, 0),
	migrations:  make(map[string]Migration, 0),
	watchers:    make(map[string]Watcher, 0),
	dispatchers: make(map[string]*changeDispatcher, 0),
}

var host bamgoo.Host

type (
	Module struct {
		mutex sync.RWMutex

		initialized bool
		connected   bool
		started     bool

		configs     Configs
		drivers     map[string]Driver
		instances   map[string]*Instance
		tables      map[string]Table
		views       map[string]View
		models      map[string]Model
		migrations  map[string]Migration
		watchers    map[string]Watcher
		dispatchers map[string]*changeDispatcher
	}

	Configs map[string]Config
	Config  struct {
		Driver      string
		Url         string
		Schema      string
		Mapping     bool
		MaxOpen     int
		MaxIdle     int
		MaxLifetime time.Duration
		MaxIdleTime time.Duration
		ReadOnly    bool
		Watcher     Map
		Migrate     MigrateOptions
		Setting     Map
	}

	Instance struct {
		conn Connection

		Name    string
		Config  Config
		Setting Map
	}
)

func (m *Module) Register(name string, value Any) {
	switch v := value.(type) {
	case Driver:
		m.RegisterDriver(name, v)
	case Config:
		m.RegisterConfig(name, v)
	case Configs:
		m.RegisterConfigs(v)
	case Table:
		m.RegisterTable(name, v)
	case View:
		m.RegisterView(name, v)
	case Model:
		m.RegisterModel(name, v)
	case Migration:
		m.RegisterMigration(name, v)
	case Watcher:
		m.RegisterWatcher(name, v)
	case Watchers:
		m.RegisterWatchers(v)
	case InsertWatcher:
		m.RegisterInsertWatcher(name, v)
	case UpdateWatcher:
		m.RegisterUpdateWatcher(name, v)
	case UpsertWatcher:
		m.RegisterUpsertWatcher(name, v)
	case DeleteWatcher:
		m.RegisterDeleteWatcher(name, v)
	}
}

func (m *Module) RegisterDriver(name string, driver Driver) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" {
		name = bamgoo.DEFAULT
	}
	if driver == nil {
		panic(errInvalidDriver)
	}
	if bamgoo.Override() {
		m.drivers[name] = driver
	} else if _, ok := m.drivers[name]; !ok {
		m.drivers[name] = driver
	}
}

func (m *Module) RegisterConfig(name string, cfg Config) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" {
		name = bamgoo.DEFAULT
	}
	if bamgoo.Override() {
		m.configs[name] = cfg
	} else if _, ok := m.configs[name]; !ok {
		m.configs[name] = cfg
	}
}

func (m *Module) RegisterConfigs(configs Configs) {
	for k, v := range configs {
		m.RegisterConfig(k, v)
	}
}

func (m *Module) RegisterTable(name string, table Table) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" {
		return
	}
	table.Name = name
	if table.Key == "" {
		table.Key = "id"
	}
	if bamgoo.Override() {
		m.tables[name] = table
	} else if _, ok := m.tables[name]; !ok {
		m.tables[name] = table
	}
}

func (m *Module) RegisterView(name string, view View) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" {
		return
	}
	view.Name = name
	if view.Key == "" {
		view.Key = "id"
	}
	if bamgoo.Override() {
		m.views[name] = view
	} else if _, ok := m.views[name]; !ok {
		m.views[name] = view
	}
}

func (m *Module) RegisterModel(name string, model Model) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" {
		return
	}
	model.Name = name
	if model.Key == "" {
		model.Key = "id"
	}
	if bamgoo.Override() {
		m.models[name] = model
	} else if _, ok := m.models[name]; !ok {
		m.models[name] = model
	}
}

func (m *Module) Config(global Map) {
	cfgAny, ok := global["data"]
	if !ok {
		return
	}
	cfgMap, ok := cfgAny.(Map)
	if !ok || cfgMap == nil {
		return
	}

	root := Map{}
	for key, val := range cfgMap {
		if item, ok := val.(Map); ok && !isDataReservedMapKey(key) {
			m.configure(key, item)
		} else {
			root[key] = val
		}
	}
	if len(root) > 0 {
		m.configure(bamgoo.DEFAULT, root)
	}
}

func (m *Module) configure(name string, cfg Map) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	out := Config{Driver: bamgoo.DEFAULT}
	if vv, ok := m.configs[name]; ok {
		out = vv
	}

	if v, ok := cfg["driver"].(string); ok && v != "" {
		out.Driver = v
	}
	if v, ok := cfg["url"].(string); ok {
		out.Url = v
	}
	if v, ok := cfg["schema"].(string); ok {
		out.Schema = v
	}
	if v, ok := cfg["mapping"]; ok {
		if vv, ok := parseBool(v); ok {
			out.Mapping = vv
		}
	}
	if v, ok := cfg["fieldMapping"]; ok {
		if vv, ok := parseBool(v); ok {
			out.Mapping = vv
		}
	}
	if v, ok := cfg["field_mapping"]; ok {
		if vv, ok := parseBool(v); ok {
			out.Mapping = vv
		}
	}
	if v, ok := cfg["readOnly"]; ok {
		if vv, ok := parseBool(v); ok {
			out.ReadOnly = vv
		}
	}
	if v, ok := cfg["readonly"]; ok {
		if vv, ok := parseBool(v); ok {
			out.ReadOnly = vv
		}
	}
	if v, ok := cfg["watcher"].(Map); ok {
		out.Watcher = v
	}
	if v, ok := parseIntAny(cfg["maxOpen"]); ok {
		out.MaxOpen = v
	}
	if v, ok := parseIntAny(cfg["max_open"]); ok {
		out.MaxOpen = v
	}
	if v, ok := parseIntAny(cfg["maxIdle"]); ok {
		out.MaxIdle = v
	}
	if v, ok := parseIntAny(cfg["max_idle"]); ok {
		out.MaxIdle = v
	}
	if v, ok := parseDurationAny(cfg["maxLifetime"]); ok {
		out.MaxLifetime = v
	}
	if v, ok := parseDurationAny(cfg["max_lifetime"]); ok {
		out.MaxLifetime = v
	}
	if v, ok := parseDurationAny(cfg["maxIdleTime"]); ok {
		out.MaxIdleTime = v
	}
	if v, ok := parseDurationAny(cfg["max_idle_time"]); ok {
		out.MaxIdleTime = v
	}
	if pool, ok := cfg["pool"].(Map); ok {
		if v, ok := parseIntAny(pool["maxOpen"]); ok {
			out.MaxOpen = v
		}
		if v, ok := parseIntAny(pool["max_open"]); ok {
			out.MaxOpen = v
		}
		if v, ok := parseIntAny(pool["maxIdle"]); ok {
			out.MaxIdle = v
		}
		if v, ok := parseIntAny(pool["max_idle"]); ok {
			out.MaxIdle = v
		}
		if v, ok := parseDurationAny(pool["maxLifetime"]); ok {
			out.MaxLifetime = v
		}
		if v, ok := parseDurationAny(pool["max_lifetime"]); ok {
			out.MaxLifetime = v
		}
		if v, ok := parseDurationAny(pool["maxIdleTime"]); ok {
			out.MaxIdleTime = v
		}
		if v, ok := parseDurationAny(pool["max_idle_time"]); ok {
			out.MaxIdleTime = v
		}
	}
	if v, ok := cfg["migrate"].(Map); ok {
		if vv, ok := v["mode"].(string); ok {
			out.Migrate.Mode = strings.ToLower(strings.TrimSpace(vv))
		}
		if vv, ok := parseBool(v["dryRun"]); ok {
			out.Migrate.DryRun = vv
		}
		if vv, ok := parseBool(v["diffOnly"]); ok {
			out.Migrate.DiffOnly = vv
		}
		if vv, ok := parseBool(v["concurrentIndex"]); ok {
			out.Migrate.Concurrent = vv
		}
		if vv, ok := parseDurationAny(v["timeout"]); ok {
			out.Migrate.Timeout = vv
		}
		if vv, ok := parseDurationAny(v["lockTimeout"]); ok {
			out.Migrate.LockTimeout = vv
		}
		if vv, ok := parseInt64(v["retry"]); ok {
			out.Migrate.Retry = int(vv)
		}
		if vv, ok := parseDurationAny(v["retryDelay"]); ok {
			out.Migrate.RetryDelay = vv
		}
		if vv, ok := parseDurationAny(v["jitter"]); ok {
			out.Migrate.Jitter = vv
		}
	}
	if v, ok := cfg["setting"].(Map); ok {
		out.Setting = v
	}

	m.configs[name] = out
}

func isDataReservedMapKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "setting", "migrate", "watcher":
		return true
	default:
		return false
	}
}

func parseDurationAny(v Any) (time.Duration, bool) {
	switch vv := v.(type) {
	case string:
		d, err := time.ParseDuration(strings.TrimSpace(vv))
		if err != nil || d <= 0 {
			return 0, false
		}
		return d, true
	case int:
		if vv <= 0 {
			return 0, false
		}
		return time.Duration(vv) * time.Millisecond, true
	case int64:
		if vv <= 0 {
			return 0, false
		}
		return time.Duration(vv) * time.Millisecond, true
	case float64:
		if vv <= 0 {
			return 0, false
		}
		return time.Duration(vv) * time.Millisecond, true
	default:
		return 0, false
	}
}

func parseIntAny(v Any) (int, bool) {
	switch vv := v.(type) {
	case int:
		if vv < 0 {
			return 0, false
		}
		return vv, true
	case int64:
		if vv < 0 {
			return 0, false
		}
		return int(vv), true
	case float64:
		if vv < 0 {
			return 0, false
		}
		return int(vv), true
	default:
		return 0, false
	}
}

func (m *Module) Setup() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.initialized {
		return
	}
	if len(m.configs) == 0 {
		m.configs[bamgoo.DEFAULT] = Config{Driver: bamgoo.DEFAULT}
	}
	for name, cfg := range m.configs {
		if name == "" {
			delete(m.configs, name)
			name = bamgoo.DEFAULT
		}
		if cfg.Driver == "" {
			cfg.Driver = bamgoo.DEFAULT
		}
		if strings.TrimSpace(cfg.Schema) == "" {
			if schema := defaultSchemaByDriver(cfg.Driver); schema != "" {
				cfg.Schema = schema
			}
		}
		m.configs[name] = cfg
	}
	m.initialized = true
}

func defaultSchemaByDriver(driver string) string {
	d := strings.ToLower(strings.TrimSpace(driver))
	switch d {
	case "postgresql", "postgres", "pgsql":
		return "public"
	default:
		return ""
	}
}

func (m *Module) Open() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.connected {
		return
	}

	for name, cfg := range m.configs {
		driver := m.drivers[cfg.Driver]
		if driver == nil {
			panic(fmt.Sprintf("invalid data driver: %s", cfg.Driver))
		}
		inst := &Instance{Name: name, Config: cfg, Setting: cfg.Setting}
		conn, err := driver.Connect(inst)
		if err != nil {
			panic("failed to connect data: " + err.Error())
		}
		if err := conn.Open(); err != nil {
			panic("failed to open data: " + err.Error())
		}
		if db := conn.DB(); db != nil {
			if cfg.MaxOpen > 0 {
				db.SetMaxOpenConns(cfg.MaxOpen)
			}
			if cfg.MaxIdle > 0 {
				db.SetMaxIdleConns(cfg.MaxIdle)
			}
			if cfg.MaxLifetime > 0 {
				db.SetConnMaxLifetime(cfg.MaxLifetime)
			}
			if cfg.MaxIdleTime > 0 {
				db.SetConnMaxIdleTime(cfg.MaxIdleTime)
			}
		}
		inst.conn = conn
		m.instances[name] = inst
		m.dispatchers[name] = newChangeDispatcher(name, m.parseChangeConfig(cfg), m)
	}

	m.connected = true
}

func (m *Module) Start() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.started {
		return
	}
	m.started = true
	fmt.Printf("bamgoo data module is running with %d connections.\n", len(m.instances))
}

func (m *Module) Stop() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if !m.started {
		return
	}
	m.started = false
}

func (m *Module) Close() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, inst := range m.instances {
		if inst.conn != nil {
			_ = inst.conn.Close()
		}
	}
	for _, dispatcher := range m.dispatchers {
		dispatcher.close()
	}
	m.instances = make(map[string]*Instance, 0)
	m.dispatchers = make(map[string]*changeDispatcher, 0)
	m.connected = false
	m.initialized = false
}

func (m *Module) GetCapabilities(names ...string) (Capabilities, error) {
	base := m.Base(names...)
	defer base.Close()
	caps := base.Capabilities()
	if caps.Dialect == "" {
		return Capabilities{}, errInvalidConnection
	}
	return caps, nil
}

func (m *Module) PoolStats(names ...string) []PoolStats {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	out := make([]PoolStats, 0, len(m.instances))
	allow := map[string]struct{}{}
	if len(names) > 0 {
		for _, name := range names {
			name = strings.TrimSpace(name)
			if name != "" {
				allow[name] = struct{}{}
			}
		}
	}

	for name, inst := range m.instances {
		if len(allow) > 0 {
			if _, ok := allow[name]; !ok {
				continue
			}
		}
		ps := PoolStats{Name: name, Driver: inst.Config.Driver}
		if db := inst.conn.DB(); db != nil {
			stats := db.Stats()
			ps.Open = stats.OpenConnections
			ps.InUse = stats.InUse
			ps.Idle = stats.Idle
			ps.WaitCount = stats.WaitCount
			ps.WaitDuration = stats.WaitDuration.Milliseconds()
			ps.MaxOpen = stats.MaxOpenConnections
		}
		ss := m.Stats(name)
		ps.Queries = ss.Queries
		ps.Writes = ss.Writes
		ps.Errors = ss.Errors
		ps.CacheHit = ss.CacheHit
		ps.CacheRate = ss.CacheRate
		ps.Slow = ss.Slow
		ps.SlowAvgMs = ss.SlowAvgMs
		ps.SlowP50Ms = ss.SlowP50Ms
		ps.SlowP95Ms = ss.SlowP95Ms
		out = append(out, ps)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
