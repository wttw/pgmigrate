package pgmigrate

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const DefaultVersionTable = "schema_version"

type Conn interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Migration struct {
	Version  int
	Filename string
}

type Migrator struct {
	VersionTable string
	Filesystem   fs.FS
	MigrateUp    []Migration
	MigrateDown  []Migration
}

// Migrate updates the target db using patches found in filesystem using DefaultVersionTable
func Migrate(ctx context.Context, filesystem fs.FS, db Conn) error {
	m, err := New(filesystem)
	if err != nil {
		return err
	}
	return m.Up(ctx, db)
}

// New returns a new Migrator based on patch scripts in filesystem
func New(filesystem fs.FS) (Migrator, error) {
	return newMigrator(filesystem, true)
}

// NewLoose returns a new Migrator based on patch scripts in filesystem, tolerating invalidly named files
func NewLoose(filesystem fs.FS) (Migrator, error) {
	return newMigrator(filesystem, false)
}

func newMigrator(filesystem fs.FS, strict bool) (Migrator, error) {
	files, err := fs.Glob(filesystem, "*.sql")
	if err != nil {
		return Migrator{}, fmt.Errorf("failed to read patches: %w", err)
	}

	m := Migrator{
		Filesystem:   filesystem,
		VersionTable: DefaultVersionTable,
	}

	pattern := regexp.MustCompile(`^([0-9]+)_(.*)\.(up|down)\.sql$`)
	for _, filename := range files {
		matches := pattern.FindStringSubmatch(filename)
		if matches == nil {
			if strict {
				return Migrator{}, fmt.Errorf("invalid filename: %s", filename)
			}
			continue
		}
		id, err := strconv.Atoi(matches[1])
		if err != nil {
			return Migrator{}, fmt.Errorf("bad filename: %s: %w", filename, err)
		}
		mig := Migration{
			Version:  id,
			Filename: filename,
		}
		if matches[3] == "up" {
			m.MigrateUp = append(m.MigrateUp, mig)
		} else {
			m.MigrateDown = append(m.MigrateDown, mig)
		}
	}
	sort.Slice(m.MigrateUp, func(i, j int) bool {
		return m.MigrateUp[i].Version < m.MigrateUp[j].Version
	})
	for i, v := range m.MigrateUp {
		if v.Version != i+1 {
			return Migrator{}, fmt.Errorf("unexpected sequence - found %s at %d", v.Filename, i+1)
		}
	}
	if m.MigrateDown == nil {
		return m, nil
	}
	sort.Slice(m.MigrateDown, func(i, j int) bool {
		return m.MigrateDown[i].Version < m.MigrateDown[j].Version
	})
	if len(m.MigrateUp) != len(m.MigrateDown) {
		return Migrator{}, fmt.Errorf("%d up scripts vs %d down scripts", len(m.MigrateUp), len(m.MigrateDown))
	}
	for i, v := range m.MigrateDown {
		if v.Version != i+1 {
			return Migrator{}, fmt.Errorf("unexpected sequence - found %s at %d", v.Filename, i+1)
		}
	}
	for i, v := range m.MigrateUp {
		if strings.TrimSuffix(v.Filename, ".up.sql") != strings.TrimSuffix(m.MigrateDown[i].Filename, ".down.sql") {
			return Migrator{}, fmt.Errorf("up/down mismatch: %s, %s", v.Filename, m.MigrateDown[i].Filename)
		}
	}
	return m, nil
}

// Latest returns the highest schema version available
func (m Migrator) Latest() int {
	if len(m.MigrateUp) == 0 {
		return 0
	}
	return m.MigrateUp[len(m.MigrateUp)-1].Version
}

// Current returns the current schema version in a database
func (m Migrator) Current(ctx context.Context, db Conn) (int, error) {
	err := m.initializeSchemaVersion(ctx, db)
	if err != nil {
		return 0, err
	}
	var version int
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, fmt.Sprintf(`select version from %s`, m.VersionTable)).Scan(&version)
	})
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve current schema version: %w", err)
	}
	return version, nil
}

func (m Migrator) Apply(ctx context.Context, db Conn, before, after int, filename string) error {
	sql, err := fs.ReadFile(m.Filesystem, filename)
	if err != nil {
		return fmt.Errorf("while reading patch file: %w", err)
	}
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		var current int
		err := tx.QueryRow(ctx, fmt.Sprintf(`select version from %s`, m.VersionTable)).Scan(&current)
		if err != nil {
			return fmt.Errorf("while reading current version: %w", err)
		}
		if current != before {
			return fmt.Errorf("expected current version %d, found %d", before, current)
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(`update %s set version = $1`, m.VersionTable), after)
		if err != nil {
			return fmt.Errorf("while updating current version: %w", err)
		}
		_, err = tx.Exec(ctx, string(sql))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("while applying %s: %w", filename, err)
	}
	return nil
}

// Up updates the database schema to the newest version
func (m Migrator) Up(ctx context.Context, db Conn) error {
	current, err := m.Current(ctx, db)
	if err != nil {
		return err
	}
	for _, v := range m.MigrateUp {
		if current >= v.Version {
			continue
		}
		err := m.Apply(ctx, db, current, v.Version, v.Filename)
		if err != nil {
			return fmt.Errorf("failed to apply %s: %w", v.Filename, err)
		}
		current = v.Version
	}
	return nil
}

func (m Migrator) initializeSchemaVersion(ctx context.Context, db Conn) error {
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY CONSTRAINT one_version CHECK(id = 1), version INTEGER NOT NULL)`, m.VersionTable))
		if err != nil {
			return err
		}
		var count int
		err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, m.VersionTable)).Scan(&count)
		if err != nil {
			return err
		}
		if count > 0 {
			// Table is already populated, so has been set up previously
			return nil
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (id, version) VALUES (1, 0)`, m.VersionTable))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("while creating version table %s: %w", m.VersionTable, err)
	}
	return nil
}
