package data

import . "github.com/bamgoo/base"

func Base(names ...string) DataBase {
	return module.Base(names...)
}

func Migrate(names ...string) error {
	db := Base()
	defer db.Close()
	db.Migrate(names...)
	return db.Error()
}

func MigrateOn(base string, names ...string) error {
	db := Base(base)
	defer db.Close()
	db.Migrate(names...)
	return db.Error()
}

func MigratePlan(names ...string) (MigrateReport, error) {
	db := Base()
	defer db.Close()
	report := db.MigratePlan(names...)
	return report, db.Error()
}

func MigratePlanOn(base string, names ...string) (MigrateReport, error) {
	db := Base(base)
	defer db.Close()
	report := db.MigratePlan(names...)
	return report, db.Error()
}

func MigrateDiff(names ...string) (MigrateReport, error) {
	db := Base()
	defer db.Close()
	report := db.MigrateDiff(names...)
	return report, db.Error()
}

func MigrateDiffOn(base string, names ...string) (MigrateReport, error) {
	db := Base(base)
	defer db.Close()
	report := db.MigrateDiff(names...)
	return report, db.Error()
}

func MigrateUp(versions ...string) error {
	db := Base()
	defer db.Close()
	db.MigrateUp(versions...)
	return db.Error()
}

func MigrateUpOn(base string, versions ...string) error {
	db := Base(base)
	defer db.Close()
	db.MigrateUp(versions...)
	return db.Error()
}

func MigrateDown(steps int) error {
	db := Base()
	defer db.Close()
	db.MigrateDown(steps)
	return db.Error()
}

func MigrateDownOn(base string, steps int) error {
	db := Base(base)
	defer db.Close()
	db.MigrateDown(steps)
	return db.Error()
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

func RegisterMigration(name string, migration Migration) {
	module.RegisterMigration(name, migration)
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

func Migrations(names ...string) []Migration {
	base := ""
	if len(names) > 0 {
		base = names[0]
	}
	return module.migrationConfigs(base)
}
