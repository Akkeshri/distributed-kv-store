package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Version map[string]uint64

type Record struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Version   Version   `json:"version"`
	Tombstone bool      `json:"tombstone"`
}

type mutation struct {
	Op        string    `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Version   Version   `json:"version"`
}

type segment struct {
	FormatVersion int      `json:"formatVersion"`
	NodeID        string   `json:"nodeId"`
	Records       []Record `json:"records"`
}

type Store struct {
	mu       sync.RWMutex
	nodeID   string
	dataDir  string
	logPath  string
	segPath  string
	records  map[string]Record
	maxKey   int
	maxValue int
}

func New(nodeID, dataDir string, maxKey, maxValue int) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	s := &Store{
		nodeID:   nodeID,
		dataDir:  dataDir,
		logPath:  filepath.Join(dataDir, "mutations.log"),
		segPath:  filepath.Join(dataDir, "snapshot.json"),
		records:  make(map[string]Record),
		maxKey:   maxKey,
		maxValue: maxValue,
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) recover() error {
	if err := s.loadSegment(); err != nil {
		return err
	}
	return s.replayLog()
}

func (s *Store) loadSegment() error {
	data, err := os.ReadFile(s.segPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read snapshot: %w", err)
	}
	var seg segment
	if err := json.Unmarshal(data, &seg); err != nil {
		return fmt.Errorf("parse snapshot: %w", err)
	}
	for _, rec := range seg.Records {
		s.records[rec.Key] = rec
	}
	return nil
}

func (s *Store) replayLog() error {
	f, err := os.Open(s.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open log: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var m mutation
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return fmt.Errorf("corrupt log line %d: %w", lineNum, err)
		}
		s.applyMutation(m)
	}
	return scanner.Err()
}

func (s *Store) applyMutation(m mutation) {
	switch m.Op {
	case "SET":
		s.records[m.Key] = Record{
			Key:       m.Key,
			Value:     m.Value,
			Timestamp: m.Timestamp,
			Version:   cloneVersion(m.Version),
			Tombstone: false,
		}
	case "DELETE":
		s.records[m.Key] = Record{
			Key:       m.Key,
			Timestamp: m.Timestamp,
			Version:   cloneVersion(m.Version),
			Tombstone: true,
		}
	}
}

func (s *Store) appendMutation(m mutation) error {
	f, err := os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log for append: %w", err)
	}
	defer f.Close()
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func (s *Store) bumpVersion(v Version, nodeID string) Version {
	out := cloneVersion(v)
	if out == nil {
		out = Version{}
	}
	out[nodeID]++
	return out
}

func (s *Store) Set(key, value string) (Record, error) {
	if err := s.validateKey(key); err != nil {
		return Record{}, err
	}
	if len(value) > s.maxValue {
		return Record{}, fmt.Errorf("value exceeds max size")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.records[key]
	rec := Record{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UTC(),
		Version:   s.bumpVersion(prev.Version, s.nodeID),
		Tombstone: false,
	}
	m := mutation{Op: "SET", Key: key, Value: value, Timestamp: rec.Timestamp, Version: rec.Version}
	if err := s.appendMutation(m); err != nil {
		return Record{}, err
	}
	s.records[key] = rec
	return rec, nil
}

func (s *Store) Delete(key string) (Record, error) {
	if err := s.validateKey(key); err != nil {
		return Record{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	prev := s.records[key]
	rec := Record{
		Key:       key,
		Timestamp: time.Now().UTC(),
		Version:   s.bumpVersion(prev.Version, s.nodeID),
		Tombstone: true,
	}
	m := mutation{Op: "DELETE", Key: key, Timestamp: rec.Timestamp, Version: rec.Version}
	if err := s.appendMutation(m); err != nil {
		return Record{}, err
	}
	s.records[key] = rec
	return rec, nil
}

func (s *Store) Get(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[key]
	if !ok || rec.Tombstone {
		return Record{}, false
	}
	return rec, true
}

func (s *Store) GetRaw(key string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[key]
	return rec, ok
}

func (s *Store) Range(start, end string) []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k, rec := range s.records {
		if rec.Tombstone {
			continue
		}
		if k >= start && (end == "" || k <= end) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		out = append(out, s.records[k])
	}
	return out
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.records))
	for k, rec := range s.records {
		if !rec.Tombstone {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) ApplyReplica(rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.records[rec.Key]
	if ok {
		if rec.Tombstone {
			if CompareVersions(rec.Version, existing.Version) <= 0 && !existing.Timestamp.Before(rec.Timestamp) {
				return nil
			}
		} else {
			if CompareVersions(rec.Version, existing.Version) < 0 {
				return nil
			}
			if CompareVersions(rec.Version, existing.Version) == 0 && existing.Timestamp.After(rec.Timestamp) {
				return nil
			}
		}
	}

	op := "SET"
	if rec.Tombstone {
		op = "DELETE"
	}
	m := mutation{Op: op, Key: rec.Key, Value: rec.Value, Timestamp: rec.Timestamp, Version: cloneVersion(rec.Version)}
	if err := s.appendMutation(m); err != nil {
		return err
	}
	s.records[rec.Key] = rec
	return nil
}

func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	recs := make([]Record, 0, len(s.records))
	for _, rec := range s.records {
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Key < recs[j].Key })

	seg := segment{FormatVersion: 1, NodeID: s.nodeID, Records: recs}
	data, err := json.MarshalIndent(seg, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.segPath, data, 0o644); err != nil {
		return err
	}
	return os.Truncate(s.logPath, 0)
}

func (s *Store) validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if len(key) > s.maxKey {
		return fmt.Errorf("key exceeds max length")
	}
	return nil
}

func CompareVersions(a, b Version) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	nodes := map[string]struct{}{}
	for k := range a {
		nodes[k] = struct{}{}
	}
	for k := range b {
		nodes[k] = struct{}{}
	}
	nodeList := make([]string, 0, len(nodes))
	for node := range nodes {
		nodeList = append(nodeList, node)
	}
	sort.Strings(nodeList)
	for _, node := range nodeList {
		av, bv := a[node], b[node]
		if av > bv {
			return 1
		}
		if av < bv {
			return -1
		}
	}
	return 0
}

func cloneVersion(v Version) Version {
	if v == nil {
		return nil
	}
	out := Version{}
	for k, val := range v {
		out[k] = val
	}
	return out
}
