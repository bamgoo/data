package data

import (
	"fmt"
	"sync"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
)

func init() {
	bamgoo.Mount(module)
}

var module = &Module{
	configs:   make(Configs, 0),
	drivers:   make(map[string]Driver, 0),
	instances: make(map[string]*Instance, 0),
	tables:    make(map[string]Table, 0),
	views:     make(map[string]View, 0),
	models:    make(map[string]Model, 0),
}

type (
	Module struct {
		mutex sync.RWMutex

		initialized bool
		connected   bool
		started     bool

		configs   Configs
		drivers   map[string]Driver
		instances map[string]*Instance
		tables    map[string]Table
		views     map[string]View
		models    map[string]Model
	}

	Configs map[string]Config
	Config  struct {
		Driver  string
		Url     string
		Schema  string
		Setting Map
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
		if item, ok := val.(Map); ok && key != "setting" {
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
	if v, ok := cfg["setting"].(Map); ok {
		out.Setting = v
	}

	m.configs[name] = out
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
		m.configs[name] = cfg
	}
	m.initialized = true
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
		inst.conn = conn
		m.instances[name] = inst
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
	m.instances = make(map[string]*Instance, 0)
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
