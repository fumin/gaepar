package gaepar

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"appengine"
	"appengine/datastore"
	"appengine/taskqueue"
)

const (
	// datastore kinds
	jobKind        = "ParJob"
	shardStateKind = "ParShardState"

	jobKeyURLValueKey            = "jobIDURLValueKey"
	shardDatastoreKeyURLValueKey = "shardDatastoreKeyURLValueKey"
)

var (
	// ErrCanceled is returned by functions like UpdateProgress to indicate that
	// a job has been canceled.
	ErrCanceled = fmt.Errorf("canceled")
)

type job struct {
	Status     string
	Heartbeat  time.Time
	Runs       int64
	MaxRetries int64
	Errors     []string `datastore:",noindex"`

	Created        time.Time
	Controller     Task   `datastore:"-"`
	ControllerJSON string `datastore:",noindex"`
	Description    string `datastore:",noindex"`

	Key *datastore.Key `datastore:"-"`
}

func (j *job) Load(c <-chan datastore.Property) error {
	if err := datastore.LoadStruct(j, c); err != nil {
		return err
	}
	cntl := Task{}
	if err := json.Unmarshal([]byte(j.ControllerJSON), &cntl); err != nil {
		return err
	}
	j.Controller = cntl
	return nil
}
func (j *job) Save(c chan<- datastore.Property) error {
	cj, err := json.Marshal(j.Controller)
	if err != nil {
		return err
	}
	j.ControllerJSON = string(cj)
	return datastore.SaveStruct(j, c)
}

func (j *job) DatastoreKey() *datastore.Key {
	return j.Key
}
func (j *job) SetStatus(s string) {
	j.Status = s
}
func (j *job) SetHeartbeat(t time.Time) {
	j.Heartbeat = t
}
func (j *job) IncrRuns() {
	j.Runs += 1
}
func (j *job) AppendError(err error) {
	j.Errors = append(j.Errors, err.Error())
}
func (j *job) GetStatus() string {
	return j.Status
}
func (j *job) GetHeartbeat() time.Time {
	return j.Heartbeat
}
func (j *job) GetRuns() int64 {
	return j.Runs
}
func (j *job) GetMaxRetries() int64 {
	return j.MaxRetries
}

type Job struct {
	Controller  Task
	MaxRetries  int64
	Description string
}

// JobKeyFromRequest returns an identifier of the currently running job.
func JobKeyFromRequest(c appengine.Context, r *http.Request) (*datastore.Key, error) {
	return datastore.DecodeKey(r.FormValue(jobKeyURLValueKey))
}

type shardState struct {
	Status     string
	Heartbeat  time.Time
	Runs       int64
	MaxRetries int64
	Errors     []string `datastore:",noindex"`

	JobID       int64
	Progress    int64  // in percentage
	Description string `datastore:",noindex"`

	key *datastore.Key `datastore:"-"`
}

func (s *shardState) DatastoreKey() *datastore.Key {
	return s.key
}
func (s *shardState) SetStatus(status string) {
	s.Status = status
}
func (s *shardState) SetHeartbeat(t time.Time) {
	s.Heartbeat = t
}
func (s *shardState) IncrRuns() {
	s.Runs += 1
}
func (s *shardState) AppendError(err error) {
	s.Errors = append(s.Errors, err.Error())
}
func (s *shardState) GetStatus() string {
	return s.Status
}
func (s *shardState) GetHeartbeat() time.Time {
	return s.Heartbeat
}
func (s *shardState) GetRuns() int64 {
	return s.Runs
}
func (s *shardState) GetMaxRetries() int64 {
	return s.MaxRetries
}

// ProgressReporter reports the progress of the current shard. Since internally
// this incurs a write to datastore, you might want to set a ThrottleDuration.
type ProgressReporter struct {
	C                appengine.Context
	R                *http.Request
	ThrottleDuration time.Duration

	progress       int64
	lastReportTime time.Time
}

// Report updates the progress of the current shard with throttling enabled.
// A return value of ErrCanceled indicates that the job has been canceled by
// user intervention, and thus callers should halt immediately.
func (pr *ProgressReporter) Report(progress int64) error {
	if progress == pr.progress || time.Now().Before(pr.lastReportTime.Add(pr.ThrottleDuration)) {
		return nil
	}
	err := pr.Put(progress)
	if err != nil {
		return err
	}
	pr.progress = progress
	pr.lastReportTime = time.Now()
	return nil
}

// Put updates the progress of the current shard without throttling.
func (pr *ProgressReporter) Put(progress int64) error {
	shardKey, err := datastore.DecodeKey(pr.R.FormValue(shardDatastoreKeyURLValueKey))
	if err != nil {
		return err
	}
	return datastore.RunInTransaction(pr.C, func(c appengine.Context) error {
		s := &shardState{}
		err1 := datastore.Get(c, shardKey, s)
		if err1 != nil {
			return err1
		}
		if s.Status == statusCanceled {
			return ErrCanceled
		}
		s.Heartbeat = time.Now()
		s.Progress = progress
		_, err1 = datastore.Put(c, shardKey, s)
		return err1
	}, nil)
}

// Task encapsulates the information needed for a job or shard to be run.
// The Values field of a Task should always be set and never be left as nil.
type Task struct {
	Values url.Values
	Path   string
	Host   string
	Delay  time.Duration
	Queue  string
}

func (t Task) add(c appengine.Context) (*taskqueue.Task, error) {
	gt := taskqueue.NewPOSTTask(t.Path, t.Values)
	gt.Header.Set("Host", t.Host)
	gt.Delay = t.Delay
	return taskqueue.Add(c, gt, t.Queue)
}

type Shard struct {
	T          Task
	MaxRetries int64
}

// CreateJob creates a job that would be executed in parallel in App Engine by
// creating the necessary datastore records and taskqueue tasks.
func CreateJob(c appengine.Context, aJob *Job, shards []Shard) (jobK *datastore.Key, _ error) {
	j := &job{
		Created:     time.Now(),
		Controller:  aJob.Controller,
		MaxRetries:  aJob.MaxRetries,
		Description: aJob.Description,
	}
	j.Controller.Delay = time.Minute
	err := datastore.RunInTransaction(c, func(tc appengine.Context) error {
		var err1 error
		jobK, err1 = datastore.Put(tc, datastore.NewIncompleteKey(tc, jobKind, nil), j)
		if err1 != nil {
			return fmt.Errorf("insert job error %v", err1)
		}
		j.Key = jobK
		j.Controller.Values.Set(jobKeyURLValueKey, j.Key.Encode())
		if _, err1 = j.Controller.add(tc); err1 != nil {
			return fmt.Errorf("job finalizer add error %v", err1)
		}
		return nil
	}, nil)
	if err != nil {
		return nil, err
	}

	ts := []struct {
		Task  *taskqueue.Task
		Queue string
	}{}
	for _, s := range shards {
		state := &shardState{
			JobID:       j.Key.IntID(),
			MaxRetries:  s.MaxRetries,
			Description: fmt.Sprintf("%+v", s),
		}
		err = datastore.RunInTransaction(c, func(tc appengine.Context) error {
			stateK, err1 := datastore.Put(tc, datastore.NewIncompleteKey(tc, shardStateKind, nil), state)
			if err1 != nil {
				return fmt.Errorf("insert shardState error %v", err1)
			}
			s.T.Values.Set(shardDatastoreKeyURLValueKey, stateK.Encode())
			s.T.Values.Set(jobKeyURLValueKey, j.Key.Encode())
			// Add a delay so that we can delete this task if there are errors later
			s.T.Delay = 50 * time.Second
			taskqueueTask, err1 := s.T.add(tc)
			if err1 != nil {
				return err1
			}
			ts = append(ts, struct {
				Task  *taskqueue.Task
				Queue string
			}{Task: taskqueueTask, Queue: s.T.Queue})
			return nil
		}, nil)
		if err != nil {
			for _, t := range ts {
				taskqueue.Delete(c, t.Task, t.Queue)
			}
			return nil, err
		}
	}
	return jobK, nil
}

// ControlJob checks the state of a job, and upon completion runs f. Callers of
// ControlJob should check for any errors returned and propagate it to App
// Engine so that the Task Queue service can retry the task.
func ControlJob(c appengine.Context, r *http.Request, f func(fc appengine.Context, fr *http.Request) error) error {
	jobK, err := datastore.DecodeKey(r.FormValue(jobKeyURLValueKey))
	if err != nil {
		return err
	}

	// Check if all shards ended. If not, resubmit taskqueue task.
	shards := []*shardState{}
	_, err = datastore.NewQuery(shardStateKind).
		Filter("JobID =", jobK.IntID()).
		GetAll(c, &shards)
	if err != nil {
		return err
	}
	for _, s := range shards {
		if !ended(s) {
			j := &job{}
			if err = datastore.Get(c, jobK, j); err != nil {
				return err
			}
			j.Key = jobK
			j.Controller.Values.Set(jobKeyURLValueKey, j.Key.Encode())
			if _, err = j.Controller.add(c); err != nil {
				return fmt.Errorf("job finalizer add error %v", err)
			}
			return nil
		}
	}

	// Halt immediately and mark the current job as failed or canceled if there
	// are shards that were not completed.
	for _, s := range shards {
		if s.Status != statusCompleted {
			return datastore.RunInTransaction(c, func(tc appengine.Context) error {
				j := &job{}
				if err1 := datastore.Get(c, jobK, j); err1 != nil {
					return err1
				}
				j.Heartbeat = time.Now()
				j.Status = s.Status
				if _, err1 := datastore.Put(c, jobK, j); err1 != nil {
					return err1
				}
				return nil
			}, nil)
		}
	}

	return run(c, r, &job{Key: jobK}, f)
}

// HandleShard handles an App Engine taskqueue request by doing the necessary
// state management first and then run f. Callers of HandleShard should check
// for any errors returned and propagate it to App Engine so that the Task Queue
// service can retry the task.
func HandleShard(c appengine.Context, r *http.Request, f func(sc appengine.Context, sr *http.Request) error) error {
	stateK, err := datastore.DecodeKey(r.FormValue(shardDatastoreKeyURLValueKey))
	if err != nil {
		return err
	}
	state := &shardState{key: stateK}
	return run(c, r, state, f)
}

type longRunningTask interface {
	DatastoreKey() *datastore.Key

	SetStatus(status string)
	SetHeartbeat(t time.Time)
	IncrRuns()
	AppendError(err error)

	GetStatus() string
	GetHeartbeat() time.Time
	GetRuns() int64
	GetMaxRetries() int64
}

const (
	statusCanceled  = "canceled"
	statusFailed    = "failed"
	statusCompleted = "completed"

	heartbeatInterval = 5 * time.Minute
)

var (
	errTaskEnded             = fmt.Errorf("task ended")
	errTaskAlreadyInProgress = fmt.Errorf("task already in progress")
)

func run(c appengine.Context, r *http.Request, v longRunningTask, f func(fc appengine.Context, fr *http.Request) error) error {
	err := datastore.RunInTransaction(c, func(tc appengine.Context) error {
		t1 := reflect.New(reflect.ValueOf(v).Elem().Type()).Interface().(longRunningTask)
		if err1 := datastore.Get(tc, v.DatastoreKey(), t1); err1 != nil {
			return err1
		}
		if ended(t1) {
			return errTaskEnded
		}
		if t1.GetHeartbeat().After(time.Now().Add(-4 * heartbeatInterval)) {
			return errTaskAlreadyInProgress
		}
		t1.SetHeartbeat(time.Now())
		t1.IncrRuns()
		if _, err1 := datastore.Put(c, v.DatastoreKey(), t1); err1 != nil {
			return err1
		}
		return nil
	}, nil)
	if err == errTaskEnded {
		return nil
	}
	if err == errTaskAlreadyInProgress {
		t, err1 := retryTaskFromRequest(c, r)
		if err1 != nil {
			c.Errorf("retryTaskFromRequest error %v", err1)
			return err1
		}
		c.Infof("retryTaskFromRequest %+v", t)
		return nil
	}
	if err != nil {
		return err
	}

	kill := putHeartbeats(c, v)
	defer func() { kill <- struct{}{} }()

	callErr := f(c, r)
	for i := 0; i != 16; i++ {
		err = datastore.RunInTransaction(c, func(tc appengine.Context) error {
			t1 := reflect.New(reflect.ValueOf(v).Elem().Type()).Interface().(longRunningTask)
			if err1 := datastore.Get(tc, v.DatastoreKey(), t1); err1 != nil {
				return err1
			}
			if t1.GetStatus() == statusCanceled {
				return nil
			}
			if callErr != nil {
				t1.AppendError(callErr)
				if t1.GetRuns() > t1.GetMaxRetries() {
					t1.SetStatus(statusFailed)
				} else {
					t1.SetHeartbeat(time.Unix(0, 0))
				}
			} else {
				t1.SetStatus(statusCompleted)
			}
			_, err1 := datastore.Put(tc, v.DatastoreKey(), t1)
			return err1
		}, nil)
		if err == nil {
			break
		}
		seconds := 2 << uint(i)
		if seconds > 60 {
			seconds = 60
		}
		time.Sleep(time.Duration(seconds) * time.Second)
	}
	return callErr
}

func putHeartbeats(c appengine.Context, v longRunningTask) chan struct{} {
	kill := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-kill:
				return
			case <-ticker.C:
				for i := 0; i < 5; i++ {
					err := datastore.RunInTransaction(c, func(tc appengine.Context) error {
						t1 := reflect.New(reflect.ValueOf(v).Elem().Type()).Interface().(longRunningTask)
						if err1 := datastore.Get(tc, v.DatastoreKey(), t1); err1 != nil {
							return err1
						}
						t1.SetHeartbeat(time.Now())
						if _, err1 := datastore.Put(tc, v.DatastoreKey(), t1); err1 != nil {
							return err1
						}
						return nil
					}, nil)
					if err == nil {
						break
					}
					time.Sleep((2 << uint(i)) * time.Second)
				}
			}
		}
	}()
	return kill
}

func retryTaskFromRequest(c appengine.Context, r *http.Request) (*taskqueue.Task, error) {
	r.ParseForm()
	host, err1 := appengine.ModuleHostname(c, "", "", "")
	if err1 != nil {
		return nil, err1
	}
	queue := r.Header.Get("X-AppEngine-QueueName")
	t := Task{Values: r.Form, Path: r.URL.Path, Host: host, Delay: heartbeatInterval, Queue: queue}
	return t.add(c)
}

func ended(t longRunningTask) bool {
	if t.GetStatus() == statusCanceled || t.GetStatus() == statusFailed || t.GetStatus() == statusCompleted {
		return true
	}
	return false
}
