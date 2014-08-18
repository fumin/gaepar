package util

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"appengine"
	"appengine/datastore"
)

// KeysOfQuery turns a query into a KeysOnly one and returns those keys.
func KeysOfQuery(c appengine.Context, q *datastore.Query, cursor *datastore.Cursor) ([]*datastore.Key, *datastore.Cursor, error) {
	q = q.KeysOnly()
	if cursor != nil {
		q = q.Start(*cursor)
	}
	t := q.Run(c)
	var keys []*datastore.Key
	for {
		k, err := t.Next(nil)
		if err == datastore.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, k)
	}
	nextCursor, err := t.Cursor()
	if err != nil {
		return nil, nil, err
	}
	return keys, &nextCursor, nil
}

func RandBytesString(l int) (string, error) {
	entropy := make([]byte, l)
	_, err := rand.Read(entropy)
	if err != nil {
		return "", err
	}
	return Base64EncodeWithoutEq(entropy), nil
}

func Base64EncodeWithoutEq(data []byte) string {
	str := base64.URLEncoding.EncodeToString(data)
	equalNum := 0
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == '=' {
			equalNum++
		} else {
			break
		}
	}
	return str[0 : len(str)-equalNum]
}

type jsonErrInfo struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
type jsonErr struct {
	Error jsonErrInfo `json:"error"`
}

func JSONError(code int, msg string) string {
	errStruct := jsonErr{
		Error: jsonErrInfo{
			Code:    code,
			Message: msg,
		},
	}
	b, err := json.Marshal(errStruct)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func HTTPBasicAuthenticated(username, password string, r *http.Request) bool {
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) != 2 {
		return false
	}
	data, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return false
	}
	up := strings.Split(string(data), ":")
	if len(up) != 2 {
		return false
	}
	if username != up[0] || password != up[1] {
		return false
	}
	return true
}

func ReadHTTPJSONBody(bodyReader io.ReadCloser, v interface{}) error {
	defer bodyReader.Close()
	body, err := ioutil.ReadAll(bodyReader)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, v)
	if err != nil {
		return fmt.Errorf(`json error %v for body %s`, err, string(body))
	}
	return nil
}

type ErrorLog struct {
	Created time.Time
	Text    string
}

func LogErrorToDatastore(c appengine.Context, text string) {
	c.Errorf(text)
	log := &ErrorLog{Created: time.Now(), Text: text}
	datastore.Put(c, datastore.NewIncompleteKey(c, "ErrorLog", nil), log)
}
