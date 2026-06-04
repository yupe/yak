package gokahelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/storage"
)

const (
	Brokers    = "localhost:9094"
	Partitions = 1
	ViewWait   = 10 * time.Second
)

var BrokerList = []string{Brokers}

func TopicManagerBuilder() goka.TopicManagerBuilder {
	cfg := goka.NewTopicManagerConfig()
	cfg.Table.Replication = 1
	cfg.Stream.Replication = 1
	return goka.TopicManagerBuilderWithTopicManagerConfig(cfg)
}

func withTopicManager() []goka.ProcessorOption {
	return []goka.ProcessorOption{goka.WithTopicManagerBuilder(TopicManagerBuilder())}
}

func EnsureTopics(stream goka.Stream, group goka.Group) error {
	tm, err := TopicManagerBuilder()(BrokerList)
	if err != nil {
		return err
	}
	defer tm.Close()

	if err := tm.EnsureStreamExists(string(stream), Partitions); err != nil {
		return fmt.Errorf("stream topic: %w", err)
	}
	if err := tm.EnsureTableExists(string(goka.GroupTable(group)), Partitions); err != nil {
		return fmt.Errorf("table %s: %w", goka.GroupTable(group), err)
	}
	return nil
}

func EnsureGroupTable(group goka.Group) error {
	return EnsureTable(goka.GroupTable(group))
}

func EnsureTable(table goka.Table) error {
	tm, err := TopicManagerBuilder()(BrokerList)
	if err != nil {
		return err
	}
	defer tm.Close()

	if err := tm.EnsureTableExists(string(table), Partitions); err != nil {
		return fmt.Errorf("table %s: %w", table, err)
	}
	return nil
}

func EnsureStreamAndTable(stream goka.Stream, table goka.Table) error {
	tm, err := TopicManagerBuilder()(BrokerList)
	if err != nil {
		return err
	}
	defer tm.Close()

	if err := tm.EnsureStreamExists(string(stream), Partitions); err != nil {
		return fmt.Errorf("stream topic: %w", err)
	}
	if err := tm.EnsureTableExists(string(table), Partitions); err != nil {
		return fmt.Errorf("table %s: %w", table, err)
	}
	return nil
}

func Emit(stream goka.Stream, c goka.Codec, key string, value interface{}) error {
	emitter, err := goka.NewEmitter(BrokerList, stream, c,
		goka.WithEmitterTopicManagerBuilder(TopicManagerBuilder()))
	if err != nil {
		return err
	}
	defer emitter.Finish()
	return emitter.EmitSync(key, value)
}

func RunConsume(stream goka.Stream, group goka.Group, c goka.Codec, cb goka.ProcessCallback) error {
	g := goka.DefineGroup(group,
		goka.Input(stream, c, cb),
		goka.Persist(c),
	)
	return RunProcessor(stream, group, g)
}

func RunProcessor(stream goka.Stream, group goka.Group, g *goka.GroupGraph) error {
	return RunProcessorWithTable(stream, goka.GroupTable(group), g)
}

func RunProcessorWithTable(stream goka.Stream, table goka.Table, g *goka.GroupGraph) error {
	if err := EnsureStreamAndTable(stream, table); err != nil {
		return err
	}

	p, err := goka.NewProcessor(BrokerList, g, withTopicManager()...)
	if err != nil {
		return err
	}
	return p.Run(context.Background())
}

func StartLookupView(group goka.Group, c goka.Codec) (*goka.View, context.CancelFunc, error) {
	return StartLookupTable(goka.GroupTable(group), c)
}

func StartLookupTable(table goka.Table, c goka.Codec) (*goka.View, context.CancelFunc, error) {
	if err := EnsureTable(table); err != nil {
		return nil, nil, err
	}
	return OpenViewTable(table, c)
}

func OpenView(group goka.Group, c goka.Codec) (*goka.View, context.CancelFunc, error) {
	return OpenViewTable(goka.GroupTable(group), c)
}

func viewStoragePath(table goka.Table) string {
	// Отдельный каталог на топик и процесс, чтобы list и consume не блокировали один LevelDB.
	return filepath.Join("/tmp/goka", "view", string(table), fmt.Sprintf("pid-%d", os.Getpid()))
}

func OpenViewTable(table goka.Table, c goka.Codec) (*goka.View, context.CancelFunc, error) {
	view, err := goka.NewView(BrokerList, table, c,
		goka.WithViewStorageBuilder(storage.DefaultBuilder(viewStoragePath(table))),
		goka.WithViewTopicManagerBuilder(TopicManagerBuilder()))
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := view.Run(ctx); err != nil {
			fmt.Printf("View error: %v\n", err)
		}
	}()

	select {
	case <-view.WaitRunning():
		return view, cancel, nil
	case <-time.After(ViewWait):
		cancel()
		return nil, nil, fmt.Errorf("view not ready: timeout (is consume running?)")
	}
}

func List(group goka.Group, c goka.Codec, title, prefix string, format func(interface{}) (string, bool)) {
	view, cancel, err := OpenView(group, c)
	if err != nil {
		fmt.Printf("Error creating view: %v\n", err)
		return
	}
	defer cancel()

	fmt.Println(title)

	var iter goka.Iterator
	if prefix == "" {
		iter, err = view.Iterator()
	} else {
		iter, err = view.IteratorWithRange(prefix, PrefixLimit(prefix))
	}
	if err != nil {
		fmt.Printf("Iterator error: %v\n", err)
		return
	}
	defer iter.Release()

	PrintIterator(iter, format)
}

func ViewHasKey(view *goka.View, key string) bool {
	val, err := view.Get(key)
	return err == nil && val != nil
}

func PrefixLimit(prefix string) string {
	if prefix == "" {
		return ""
	}
	last := prefix[len(prefix)-1]
	if last < 0xff {
		return prefix[:len(prefix)-1] + string(last+1)
	}
	return prefix + "\x00"
}

func PrintIterator(iter goka.Iterator, format func(interface{}) (string, bool)) {
	count := 0
	for iter.Next() {
		val, err := iter.Value()
		if err != nil {
			fmt.Printf("Iterator value error: %v\n", err)
			return
		}
		if s, ok := format(val); ok {
			fmt.Printf("  - %s\n", s)
			count++
		}
	}
	if err := iter.Err(); err != nil {
		fmt.Printf("Iterator error: %v\n", err)
		return
	}
	if count == 0 {
		fmt.Println("  (none)")
	}
}
