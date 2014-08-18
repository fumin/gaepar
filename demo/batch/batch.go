package batch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"appengine"
	"appengine/file"
	"appengine/urlfetch"

	"code.google.com/p/goauth2/appengine/serviceaccount"
	"code.google.com/p/google-api-go-client/storage/v1"

	"github.com/fumin/gaepar"
)

func init() {
	http.HandleFunc("/Repo", Repo)
	http.HandleFunc("/CopyReposToBigquery", CopyReposToBigquery)
	http.HandleFunc("/CopyReposToBigqueryShard", CopyReposToBigqueryShard)
	http.HandleFunc("/CopyReposToBigqueryControl", CopyReposToBigqueryControl)

	http.HandleFunc("/_ah/start", ahStart)
	http.HandleFunc("/_ah/stop", ahStop)
}

var (
	shutdownListeners = struct {
		sync.RWMutex
		m map[int]chan struct{}
	}{m: make(map[int]chan struct{})}
)

func listenShutdown() (int, chan struct{}) {
	shutdownListeners.Lock()
	defer shutdownListeners.Unlock()
	shutdownKey := rand.Int()
	shutdown := make(chan struct{}, 1)
	shutdownListeners.m[shutdownKey] = shutdown
	return shutdownKey, shutdown
}

func unlistenShutdown(k int) {
	shutdownListeners.Lock()
	defer shutdownListeners.Unlock()
	delete(shutdownListeners.m, k)
}

func Repo(w http.ResponseWriter, r *http.Request) {
	created, err := time.Parse("2006-01-02", r.FormValue("created"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c := appengine.NewContext(r)

	cl := urlfetch.Client(c)
	b, err := repoCreatedAfter(cl, created)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(b)
}

type copyReposToBigqueryValues struct {
	Start          time.Time
	End            time.Time
	Shards         int64
	ShardWidth     time.Duration
	ModuleHostname string
}

func copyReposToBigqueryFormValues(c appengine.Context, r *http.Request) (*copyReposToBigqueryValues, error) {
	v := &copyReposToBigqueryValues{}
	startStr := r.FormValue("start")
	var err error
	if v.Start, err = time.Parse("2006-01-02", startStr); err != nil {
		return nil, fmt.Errorf("wrong date format for start: %s", startStr)
	}
	endStr := r.FormValue("end")
	if v.End, err = time.Parse("2006-01-02", endStr); err != nil {
		return nil, fmt.Errorf("wrong date format for end: %s", endStr)
	}
	v.Shards, err = strconv.ParseInt(r.FormValue("shards"), 10, 0)
	if err != nil {
		v.Shards = 100
	}

	v.ShardWidth = time.Duration(int64(v.End.Sub(v.Start))/v.Shards + 1)
	if v.ShardWidth <= 0 {
		return nil, fmt.Errorf("non-positive shard width %d for start %v end %v", v.ShardWidth, v.Start, v.End)
	}
	v.ModuleHostname, err = appengine.ModuleHostname(c, appengine.ModuleName(c), "", "")
	if err != nil {
		return nil, fmt.Errorf("cannot get current module hostname %v", err)
	}
	return v, nil
}

func CopyReposToBigquery(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	v, err := copyReposToBigqueryFormValues(c, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	shards := []gaepar.Shard{}
	for i := v.Start; i.Before(v.End); i = i.Add(v.ShardWidth) {
		subEnd := i.Add(v.ShardWidth)
		if subEnd.After(v.End) {
			subEnd = v.End
		}
		t := gaepar.Task{Values: url.Values{}}
		t.Values.Set("start", i.Format("2006-01-02"))
		t.Values.Set("end", subEnd.Format("2006-01-02"))
		t.Path = "/CopyReposToBigqueryShard"
		t.Host = v.ModuleHostname
		t.Queue = "CopyReposToBigquery"
		shards = append(shards, gaepar.Shard{T: t, MaxRetries: 3})
	}

	descr, err := json.Marshal(v)
	if err != nil {
		http.Error(w, fmt.Sprintf("cannot json job values %+v", v), http.StatusInternalServerError)
		return
	}
	cntl := gaepar.Task{Values: url.Values{}}
	cntl.Path = "/CopyReposToBigqueryControl"
	cntl.Host = v.ModuleHostname
	job := &gaepar.Job{Controller: cntl, Description: string(descr), MaxRetries: 3}
	jobKey, err := gaepar.CreateJob(c, job, shards)
	if err != nil {
		http.Error(w, fmt.Sprintf("CreateJob error %v", err), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(fmt.Sprintf("job key: %v", jobKey)))
}

func CopyReposToBigqueryShard(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	err := gaepar.HandleShard(c, r, func(fc appengine.Context, fr *http.Request) error {
		start, err1 := time.Parse("2006-01-02", r.FormValue("start"))
		if err1 != nil {
			return err1
		}
		end, err1 := time.Parse("2006-01-02", r.FormValue("end"))
		if err1 != nil {
			return err1
		}
		jobKey, err1 := gaepar.JobKeyFromRequest(fc, fr)
		if err1 != nil {
			return err1
		}
		service, err1 := newStorageService(fc, storage.DevstorageRead_writeScope)
		if err1 != nil {
			return err1
		}
		objService := storage.NewObjectsService(service)
		defaultBucket, err1 := file.DefaultBucketName(fc)
		if err1 != nil {
			return err1
		}
		obj := &storage.Object{
			Name:        fmt.Sprintf("CopyReposToBigquery/%d/shards/%s_%s.json.bigquery", jobKey.IntID(), start.Format("2006-01-02"), end.Format("2006-01-02")),
			ContentType: "text/plain; charset=utf-8",
		}
		pr, pw := io.Pipe()
		call := objService.Insert(defaultBucket, obj).Media(pr)
		uploadDone := make(chan error)
		go func() {
			_, err := call.Do()
			uploadDone <- err
		}()

		n := 2
		cl := urlfetch.Client(c)
		repoCreated := make(chan time.Time)
		fetchDone := make(chan error)
		for i := 0; i < n; i++ {
			go func() {
				for created := range repoCreated {
					j, err := repoCreatedAfter(cl, created)
					if appengine.IsOverQuota(err) {
						c.Errorf("repoCreatedAfter over quota %v", err)
						time.Sleep(90 * time.Second)
					}
					if err == nil {
						pw.Write(append(j, '\n'))
					}
				}
				fetchDone <- nil
			}()
		}

		k, shutdown := listenShutdown()
		defer unlistenShutdown(k)
		var childErr error
		reporter := &gaepar.ProgressReporter{C: c, R: r, ThrottleDuration: 5 * time.Second}
	L:
		for i := start; i.Before(end); i = i.Add(24 * time.Hour) {
			select {
			case childErr = <-uploadDone:
				break L
			case childErr = <-fetchDone:
				break L
			case <-shutdown:
				return fmt.Errorf("instance shutdown")
			default:
				repoCreated <- i
				progress := int64(i.Sub(start) * 100 / (end.Sub(start)))
				err1 := reporter.Report(progress)
				if err1 == gaepar.ErrCanceled {
					break L
				}
			}
		}
		close(repoCreated)
		for i := 0; i < n; i++ {
			childErr = <-fetchDone
		}
		pw.Close()
		childErr = <-uploadDone

		if childErr != nil {
			return childErr
		}
		reporter.Put(100)
		return nil
	})
	if err != nil {
		c.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func CopyReposToBigqueryControl(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	err := gaepar.ControlJob(c, r, func(fc appengine.Context, fr *http.Request) error {
		jobKey, err := gaepar.JobKeyFromRequest(fc, fr)
		if err != nil {
			return err
		}
		service, err := newStorageService(fc, storage.DevstorageRead_writeScope)
		if err != nil {
			return err
		}
		objService := storage.NewObjectsService(service)
		defaultBucket, err := file.DefaultBucketName(c)
		if err != nil {
			return err
		}

		// List the resulting objects of each shard
		objs := []*storage.Object{}
		prefix := fmt.Sprintf("CopyReposToBigquery/%d/shards/", jobKey.IntID())
		pageToken := ""
		for {
			objects, err := objService.List(defaultBucket).Delimiter("/").Prefix(prefix).PageToken(pageToken).Do()
			if err != nil {
				return err
			}
			objs = append(objs, objects.Items...)
			if objects.NextPageToken == "" {
				break
			}
			pageToken = objects.NextPageToken
		}
		if len(objs) == 0 {
			return nil
		}

		// Concatenate the results of each shard into one output file
		dest := fmt.Sprintf("CopyReposToBigquery/%d/data.json.bigquery", jobKey.IntID())
		obj := &storage.Object{Name: dest, ContentType: objs[0].ContentType}
		if _, err := objService.Insert(defaultBucket, obj).Media(bytes.NewBuffer([]byte{})).Do(); err != nil {
			return err
		}
		req := &storage.ComposeRequest{
			SourceObjects: []*storage.ComposeRequestSourceObjects{},
			Destination:   &storage.Object{ContentType: objs[0].ContentType},
		}
		for i, o := range objs {
			req.SourceObjects = append(req.SourceObjects, &storage.ComposeRequestSourceObjects{Name: o.Name})
			if len(req.SourceObjects) == 31 || i == len(objs)-1 {
				req.SourceObjects = append(req.SourceObjects, &storage.ComposeRequestSourceObjects{Name: dest})
				if _, err = objService.Compose(defaultBucket, dest, req).Do(); err != nil {
					c.Errorf("compose error: %v, req: %+v", err, req)
					return err
				}
				req.SourceObjects = []*storage.ComposeRequestSourceObjects{}
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ahStart(w http.ResponseWriter, r *http.Request) {
}

func ahStop(w http.ResponseWriter, r *http.Request) {
	shutdownListeners.RLock()
	defer shutdownListeners.RUnlock()
	c := appengine.NewContext(r)
	c.Infof("shutting down listeners")
	for k, ch := range shutdownListeners.m {
		ch <- struct{}{}
		c.Infof("shutdown %d", k)
	}
}

func repoCreatedAfter(c *http.Client, created time.Time) ([]byte, error) {
	// Sleep a while so that we can see the progress of shards advancing,
	// as well as to avoid exceeding the github API rate limit
	time.Sleep(20 * time.Second)

	resp, err := c.Get(fmt.Sprintf("https://api.github.com/search/repositories?q=created:%s", created.Format("2006-01-02")))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	jsonResp := struct {
		Items []struct {
			ID    int64  `json:"id"`
			Name  string `json:"name"`
			Owner struct {
				Login string `json:"login"`
				ID    int64  `json:"id"`
			} `json:"owner"`
			Fork            bool      `json:"fork"`
			CreatedAt       time.Time `json:"created_at"`
			UpdatedAt       time.Time `json:"updated_at"`
			PushedAt        time.Time `json:"pushed_at"`
			Size            int64     `json:"size"`
			StargazersCount int64     `json:"stargazers_count"`
			WatchesCount    int64     `json:"watches_count"`
			Language        string    `json:"language"`
			ForksCount      int64     `json:"forks_count"`
			DefaultBranch   string    `json:"default_branch"`
			Score           float64   `json:"score"`
		} `json:"items"`
	}{}
	if err = json.Unmarshal(body, &jsonResp); err != nil {
		return nil, err
	}
	if len(jsonResp.Items) == 0 {
		return nil, fmt.Errorf("no repos for created %s", created.Format("2006-01-02"))
	}
	repo := jsonResp.Items[0]
	return json.Marshal(repo)
}

func newStorageService(c appengine.Context, scope string) (*storage.Service, error) {
	client, err := serviceaccount.NewClient(c, scope)
	if err != nil {
		return nil, fmt.Errorf("failed to create service account client: %v", err)
	}
	service, err := storage.New(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage service: %v", err)
	}
	return service, nil
}
