package data

import . "github.com/bamgoo/base"

func Base(names ...string) DataBase {
	return module.Base(names...)
}

func GetCapabilities(names ...string) (Capabilities, error) {
	return module.GetCapabilities(names...)
}

func GetStats(names ...string) Stats {
	return module.Stats(names...)
}

func RegisterDriver(name string, driver Driver) {
	module.RegisterDriver(name, driver)
}

func RegisterConfig(name string, cfg Config) {
	module.RegisterConfig(name, cfg)
}

func RegisterTable(name string, table Table) {
	module.RegisterTable(name, table)
}

func RegisterView(name string, view View) {
	module.RegisterView(name, view)
}

func RegisterModel(name string, model Model) {
	module.RegisterModel(name, model)
}

func Parse(args ...Any) (Query, error) {
	return ParseQuery(args...)
}

func Tables() map[string]Table {
	return module.Tables()
}

func Views() map[string]View {
	return module.Views()
}

func Models() map[string]Model {
	return module.Models()
}
