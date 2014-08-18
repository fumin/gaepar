gaepar
------
parallel job execution on Google App Engine

### Documentation
http://godoc.org/github.com/fumin/gaepar

### Demo
This demo provides an example of collecting github repositories in parallel.
In particular, we will be collecting one repo for each day in the interval
2014-01-01 to 2014-07-01, in total 181 days. We will be doing this with
20 shards running in parallel, which most probably would be run on two more
instances given the App Engine policy of restricting the concurrency of an
instance to 10.
#### Run development server
`dev_appserver.py --enable_sendmail=yes --datastore_path=datastore.db demo/app.yaml batch/app.yaml`
#### Deploy
`appcfg.py update module_to_be_uploaded/ --no_cookies --application=app_id`
#### Submit jobs
* Test our implementation of finding a github repo that is created on a day say 2014-01-01 `https://batch-dot-imposing-muse-664.appspot.com/Repo?created=2014-01-01`
* Submit a job with 20 shards `https://batch-dot-imposing-muse-664.appspot.com/CopyReposToBigquery?start=2014-01-01&end=2014-07-01&shards=20`. This job might take a minute or two. While it is running, observe that the job is distributed to more than one instance.
* When the job is completed, a resulting file would be created in Google Cloud Storage with the URL gs://app_id.appspot.com/CopyReposToBigquery/job_id/data.json.bigquery. Load the resulting file into Google Bigquery with the following schema:
```
[
    {
        "name": "id",
        "type": "INTEGER",
        "mode": "REQUIRED"
    },
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "owner",
        "type": "RECORD",
        "mode": "REQUIRED",
        "fields": [
            {
                "name": "login",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "id",
                "type": "INTEGER",
                "mode": "REQUIRED"
            }
        ]
    },
    {
        "name": "fork",
        "type": "BOOLEAN"
    },
    {
        "name": "created_at",
        "type": "TIMESTAMP",
        "mode": "REQUIRED"
    },
    {
        "name": "updated_at",
        "type": "TIMESTAMP"
    },
    {
        "name": "pushed_at",
        "type": "TIMESTAMP"
    },
    {
        "name": "size",
        "type": "INTEGER"
    },
    {
        "name": "stargazers_count",
        "type": "INTEGER"
    },
    {
        "name": "watches_count",
        "type": "INTEGER"
    },
    {
        "name": "language",
        "type": "STRING"
    },
    {
        "name": "forks_count",
        "type": "INTEGER"
    },
    {
        "name": "default_branch",
        "type": "STRING"
    },
    {
        "name": "score",
        "type": "FLOAT"
    }
]
```
* Enjoy the data that is loaded into Bigquery, for example by asserting that the number of rows is 181 (number of days between 2014-01-01 and 2014-07-01) or by running other more interesting queries.
