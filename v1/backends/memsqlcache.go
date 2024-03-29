package backends

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/paulm/memsqlcache"
)

// MemsqlCacheBackend represents a Memsqlcache result backend
type MemsqlCacheBackend struct {
	Backend
	ConnString string
	client     *memsqlcache.Client
}

// NewMemsqlCacheBackend creates MemsqlCacheBackend instance
func NewMemsqlCacheBackend(cnf *config.Config, connString string) Interface {
	return &MemsqlCacheBackend{
		Backend:    New(cnf),
		ConnString: connString,
	}
}

// InitGroup creates and saves a group meta data object
func (b *MemsqlCacheBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&memsqlcache.Item{
		Key:        groupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// GroupCompleted returns true if all tasks in a group finished
func (b *MemsqlCacheBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *MemsqlCacheBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *MemsqlCacheBackend) TriggerChord(groupUUID string) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// If group meta is locked, wait until it's unlocked
	for groupMeta.Lock {
		groupMeta, _ = b.getGroupMeta(groupUUID)
		log.WARNING.Print("Group meta locked, waiting")
		<-time.After(time.Millisecond * 5)
	}

	// Acquire lock
	if err = b.lockGroupMeta(groupMeta); err != nil {
		return false, err
	}
	defer b.unlockGroupMeta(groupMeta)

	// Update the group meta data
	groupMeta.ChordTriggered = true
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}
	if err = b.getClient().Replace(&memsqlcache.Item{
		Key:        groupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	}); err != nil {
		return false, err
	}

	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *MemsqlCacheBackend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *MemsqlCacheBackend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *MemsqlCacheBackend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *MemsqlCacheBackend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *MemsqlCacheBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *MemsqlCacheBackend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *MemsqlCacheBackend) GetState(taskUUID string) (*tasks.TaskState, error) {
	item, err := b.getClient().Get(taskUUID)
	if err != nil {
		return nil, err
	}

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item.Value))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *MemsqlCacheBackend) PurgeState(taskUUID string) error {
	return b.getClient().Delete(taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *MemsqlCacheBackend) PurgeGroupMeta(groupUUID string) error {
	return b.getClient().Delete(groupUUID)
}

// updateState saves current task state
func (b *MemsqlCacheBackend) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	return b.getClient().Set(&memsqlcache.Item{
		Key:        taskState.TaskUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// lockGroupMeta acquires lock on group meta data
func (b *MemsqlCacheBackend) lockGroupMeta(groupMeta *tasks.GroupMeta) error {
	groupMeta.Lock = true
	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&memsqlcache.Item{
		Key:        groupMeta.GroupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// unlockGroupMeta releases lock on group meta data
func (b *MemsqlCacheBackend) unlockGroupMeta(groupMeta *tasks.GroupMeta) error {
	groupMeta.Lock = false
	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&memsqlcache.Item{
		Key:        groupMeta.GroupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *MemsqlCacheBackend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.getClient().Get(groupUUID)
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item.Value))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *MemsqlCacheBackend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	states := make([]*tasks.TaskState, len(taskUUIDs))

	for i, taskUUID := range taskUUIDs {
		item, err := b.getClient().Get(taskUUID)
		if err != nil {
			return nil, err
		}

		state := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(item.Value))
		decoder.UseNumber()
		if err := decoder.Decode(state); err != nil {
			return nil, err
		}

		states[i] = state
	}

	return states, nil
}

// getExpirationTimestamp returns expiration timestamp
func (b *MemsqlCacheBackend) getExpirationTimestamp() int32 {
	expiresIn := b.cnf.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	return int32(time.Now().Unix() + int64(expiresIn))
}

// getClient returns or creates instance of Memsqlcache client
func (b *MemsqlCacheBackend) getClient() *memsqlcache.Client {
	if b.client == nil {
		b.client = memsqlcache.New(b.ConnString)
	}

	return b.client
}
