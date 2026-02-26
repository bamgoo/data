package data

import (
	"fmt"
)

func (m *Module) tableConfig(base, name string) (Table, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, key := range []string{base + "." + name, "*." + name, name} {
		if cfg, ok := m.tables[key]; ok {
			return cfg, nil
		}
	}
	return Table{}, fmt.Errorf("data table not found: %s", name)
}

func (m *Module) viewConfig(base, name string) (View, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, key := range []string{base + "." + name, "*." + name, name} {
		if cfg, ok := m.views[key]; ok {
			return cfg, nil
		}
	}
	return View{}, fmt.Errorf("data view not found: %s", name)
}

func (m *Module) modelConfig(base, name string) (Model, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, key := range []string{base + "." + name, "*." + name, name} {
		if cfg, ok := m.models[key]; ok {
			return cfg, nil
		}
	}
	return Model{}, fmt.Errorf("data model not found: %s", name)
}

func (m *Module) Tables() map[string]Table {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make(map[string]Table, len(m.tables))
	for k, v := range m.tables {
		out[k] = v
	}
	return out
}

func (m *Module) Views() map[string]View {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make(map[string]View, len(m.views))
	for k, v := range m.views {
		out[k] = v
	}
	return out
}

func (m *Module) Models() map[string]Model {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make(map[string]Model, len(m.models))
	for k, v := range m.models {
		out[k] = v
	}
	return out
}
