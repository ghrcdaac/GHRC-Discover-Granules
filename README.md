[![Coverage Status](https://coveralls.io/repos/github/ghrcdaac/GHRC-Discover-Granules/badge.svg)](https://coveralls.io/github/ghrcdaac/GHRC-Discover-Granules)
![Build Status](https://github.com/ghrcdaac/GHRC-Discover-Granules/actions/workflows/python-package.yml/badge.svg?branch=master)
```text
    _____ _    _ _____   _____      _____  _                                     _____                       _           
  / ____| |  | |  __ \ / ____|    |  __ \(_)                                   / ____|                     | |          
 | |  __| |__| | |__) | |   ______| |  | |_ ___  ___ _____   _____ _ __ ______| |  __ _ __ __ _ _ __  _   _| | ___  ___ 
 | | |_ |  __  |  _  /| |  |______| |  | | / __|/ __/ _ \ \ / / _ \ '__|______| | |_ | '__/ _` | '_ \| | | | |/ _ \/ __|
 | |__| | |  | | | \ \| |____     | |__| | \__ \ (_| (_) \ V /  __/ |         | |__| | | | (_| | | | | |_| | |  __/\__ \
  \_____|_|  |_|_|  \_\\_____|    |_____/|_|___/\___\___/ \_/ \___|_|          \_____|_|  \__,_|_| |_|\__,_|_|\___||___/
```
# Overview
The GHRC Discover Granules terraform module uses a lambda function to discover granules at HTTP/HTTPS, SFTP and S3 providers. 

The code retrieves the granule names, ETag, Last-Modified, and size values from the provider location and generates
output to be used in the Cumulus QueueGranules lambda. An attempt is made to generate output that mirrors that of the 
cumulus discover granules schema: https://github.com/nasa/cumulus/blob/master/tasks/discover-granules/schemas/output.json  

ETag: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag  
Last-Modified: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified  

## Features
 - AWS S3, HTTP/HTTPS, and SFTP discovery support
 - Recursive discovery for HTTP/S and SFTP
 - Timeout aware S3 discovery capacity (1)
 - Batching (1)
 - Discovering granules in external buckets with provided access key

(1): Requires state machine definition modifications mentioned below

## Versioning
We are following `v<major>.<minor>.<patch>` versioning convention, where:
* `<major>+1` means we changed the infrastructure and/or the major components that makes this software run. Will definitely 
  lead to breaking changes.
* `<minor>+1` means we upgraded/patched the dependencies this software relays on. Can lead to breaking changes.
* `<patch>+1` means we fixed a bug and/or added a feature. Breaking changes are not expected.

# 🔨 Pre-requisite 
## Cumulus
This module is meant to run with the Cumulus stack. 
If you don't have Cumulus stack deployed yet please consult [this repo](https://github.com/nasa/cumulus) 
and follow the [documentation](https://nasa.github.io/cumulus/docs/cumulus-docs-readme) to provision it.  

## Database Configuration Options
As of v4.0.0 the module supports two database configurations though the `db_type` terraform variable.   

### AWS RDS Aurora-Postgresql: `db_type="postgresql"` 
An RDS instance will be created exclusively for this module. Much of the parameters are configurable through terraform 
variables so that the deployment can be customized as needed. This deployment offers the most flexibility as it is still
possible to use the cumulus database as read only for smaller discovery efforts. It is important to note that if the
`db_type` variable is changed on subsequent deployments the contents of the postgresql database will be lost.

### Cumulus-Read-Only: `db_type="cumulus"`
When running with `"duplicateHandling": "skip"` the code will check the discovered `granule_id`s against the cumulus
database to see if it can skip generating and returning output for that granule. If `"duplicateHandling": "replace"` is 
used instead, then no reads will be performed against the database.  
There is some concern that large queries against the cumulus database could cause resource starvation for the cumulus
infrastructure, so bear this in mind when using `skip` with this configuration.  
It is also worth noting that this configuration does not support batching. 

# How to
In order to use the GDG the following block must be added to the collection definition inside the `meta.collection.meta`
block:
```json
{
  "discover_tf": {
    "cumulus_filter": true,
    "depth": 0,
    "force_replace": false,
    "dir_reg_ex": "",
    "file_reg_ex": "",
    "batch_limit": 1000,
    "batch_delay": 0,
    "file_count": 1,
    "ignore_discovered": false
  }
}
``` 
 - `cumulus_filter`: If set to `true` and the collection's duplicateHandling is set to `skip` GDG will attempt
   to filter discovered granules against the cumulus database and only discover granules that do not exist.
   
 - `depth`: How far you want the recursive search to go from the starting URL. The search will look for granules in each level
and traverse directories until there are no directories or depth is reached. This has no meaning for S3 providers.  
Note: The absolute value will be taken of this parameter so negative values are not intended to be used for upward traversal.

 - `force_replace`: This can be used to force Discover Granules to rediscover all granules even if previously discovered.
The duplicateHandling flag being set to replace defaults to "skip" to handle reingesting previously discovered files that 
have been updated. This has no effect when using `cumulus_filter: true`

 - `dir_reg_ex`: Regular expression used to only search directories it matches

 - `granule_id`: Override for the cumulus granuleId to allow for dynamic pattern substitution

 - `granule_id_extraction`: Override for the cumulus granuleIdExtraction to allow for dynamic pattern substitution

 - `batch_limit`: Used to specify the size of the batches returned from the lambda. To take advantage of this the 
   workflow definition will need to be modified to include a choice step to ensure all the granules are discovered
   and queued.
 - `batch_delay`: If this is provided, the workflow can use a `WaitStep` and wait for the specified duration before continuing to the next workflow step.
 - `file_count`: Used to indicate how many files are expected to be part of a discovered granule. Output will not be generated for granules with a file count less than this. A default value of 1 is used.
 - `ignore_discovered`: This will cause any record with a status of `discovered` that matches the provder path and collection ID to be set to `ignored`. This will allow for a rediscovery of all records but also handle instances where files are in a `discovered` state but have been moved. This will only occur on the initial run in an execution.

row | use_cumulus_filter |	duplicateHandling |	force_replace |	ingest | gdg writes
:---: | :---: | :---: | :---: |:---: |:---: 
1 | true | skip | true | new/updated | replace
2 | true | skip | false | new/updated | replace
3 | true | replace | true | all discovered | replace
4 | true | replace | false | all discovered | replace
5 | false | skip | true | new/updated* | skip
6 | false | skip | false | new/updated* | skip
7 | false | replace | true | all discovered | replace
8 | false | replace | false | new/updated | skip

*Note: Cumulus will raise an error if a granuleId is ingested that already exists in the database while using "duplicateHandling": "skip"

In order to match against specific granules the granuleIdExtraction value must be used.  
This is an example of a collection with the added block:
 ```json
{
  "name": "msutls",
  "version": "6",
  "dataType": "msut",
  "process": "msut",
  "url_path": "msutls__6",
  "duplicateHandling": "replace",
  "granuleId": "^(tls|uah).*_6\\.0\\.(nc|txt)$",
  "granuleIdExtraction": "((tls|uah).*_6\\.0(\\.nc)?)",
  "reportToEms": true,
  "sampleFileName": "tlsmonamg.2019_6.0.nc",
  "files": [
    {
      "bucket": "protected",
      "regex": "^(tls|uah).*_6\\.0(\\.nc|\\.txt)?$",
      "sampleFileName": "uahncdc_ls_6.0.txt",
      "reportToEms": true
    },
    {
      "bucket": "public",
      "regex": "^(tls|uah).*_6\\.0\\.(nc|txt)\\.cmr\\.xml$",
      "sampleFileName": "tlsmonamg.1999_6.0.nc.cmr.xml",
      "reportToEms": true
    }
  ],
  "meta": {
    "discover_tf": {
      "depth": 0,
      "dir_reg_ex": "",
      "force_replace": true,
      "batch_limit": 1000,
      "batch_delay": 0
    },
    "hyrax_processing": "false",
    "provider_path": "/public/msu/v6.0/tls/",
    "collection_type": "ongoing",
    "metadata_extractor": [
      {
        "regex": "^.*(nc|txt)$",
        "module": "netcdf"
      }
    ]
  }
}
```

The last relevant value in the collection definition is "duplicateHandling".  The value is used to tell 
ghrc-discover-granules how to handle granules that already exist in the configured database but also are discovered on the
current run. Discover granules handles the following possible values:
 - skip: Overwrite the ETag or Last-Modified values pulled from S3 if they differ from what the provider returns for 
   this run
 - replace: Existing granule records in the database will be ignored and discovered as if they were new

# Lambda Output
The following is an example of the modified `discover_tf` block that the GDG lambda will produce:
```json
{
  "discover_tf": {
    "depth": 0,
    "dir_reg_ex": ".*",
    "batch_delay": 1,
    "batch_limit": 1234,
    "force_replace": false,
    "cumulus_filter": true,
    "skip_queue_granules": true,
    "batch_size": 1234,
    "discovered_files_count": 35563,
    "queued_files_count": 1234,
    "queued_granules_count": 1234,
    "bookmark": null
  }
}
```
This will be present at `meta.collection.meta.discover_tf`

# Payload Output
The module generates output that should match the output for the Cumulus DiscoverGranules 
lambda: https://github.com/nasa/cumulus/tree/master/tasks/discover-granules   
Here is a sample excerpt from the database:  
```
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210501v7.gz,"e636b16d603fd71:0",2021-05-02 14:35:42+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210501v7_d3d.gz,"bf74b470603fd71:0",2021-05-02 14:35:47+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210502v7.gz,"4d338a972940d71:0",2021-05-03 14:35:41+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210502v7_d3d.gz,"c6f29b982940d71:0",2021-05-03 14:35:42+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210503v7.gz,"65f3f5cff040d71:0",2021-05-04 14:21:45+00:00  
```

The step function returns a list of dictionaries of granules that were discovered this run. 
This is an example of one of the dictionary entries:   
```json
[
  {
    "granuleId": "f16_20210601v7.gz",
    "dataType": "rssmif16d",
    "version": "7",
    "files": [
      {
        "name": "f16_20210601v7.gz",
        "path": "/ssmi/f16/bmaps_v07/y2021/m06/",
        "size": "",
        "time": 1622743794.0,
        "bucket": "some-bucket",
        "url_path": "rssmif16d__7",
        "type": ""
      }
    ]
  }
]
```
Note: The actual output uses single quotes but double quotes were used here to avoid syntax error highlighting.

This will be added to the input event at `payload.granules`

# Concurrent Workflow Executions
While it is not intended, it is possible to run multiple workflows for the same collection. There should not be any
errors or failures while doing this but just know that the file counts in the GDG lambda output may end up in 
unusual states. For example, the workflow can complete with the `queued_files_count` being less than or greater than the 
`discovered_file_count` or the lambda could do a superfluous execution that will return an empty payload before 
completing. 
   
# Step Configuration
The following definition is an example of defining the lambda as a step in an AWS state machine. This configuration is read by the CMA which can be read about here: https://nasa.github.io/cumulus/docs/workflows/input_output
GHRCDiscoverGranules definition:
```json
{
  "GHRCDiscoverGranulesLambda": {
    "Parameters": {
      "cma": {
        "event.$": "$",
        "ReplaceConfig": {
          "FullMessage": true
        },
        "task_config": {
          "provider": "{$.meta.provider}",
          "provider_path": "{$.meta.collection.meta.provider_path}",
          "collection": "{$.meta.collection}",
          "buckets": "{$.meta.buckets}",
          "stack": "{$.meta.stack}",
          "duplicateGranuleHandling": "{$.meta.collection.duplicateHandling}",
          "cumulus_message": {
            "outputs": [
              {
                "source": "{$.batch_size}",
                "destination": "{$.meta.collection.meta.discover_tf.batch_size}"
              },
              {
                "source": "{$.discovered_files_count}",
                "destination": "{$.meta.collection.meta.discover_tf.discovered_files_count}"
              },
              {
                "source": "{$.queued_files_count}",
                "destination": "{$.meta.collection.meta.discover_tf.queued_files_count}"
              },
              {
                "source": "{$.queued_granules_count}",
                "destination": "{$.meta.collection.meta.discover_tf.queued_granules_count}"
              },
              {
                "source": "{$.granules}",
                "destination": "{$.payload.granules}"
              }
            ]
          }
        }
      }
    },
    "Type": "Task",
    "Resource": "${ghrc-discover-granules_arn}",
    "Retry": [
      {
        "ErrorEquals": [
          "Lambda.ServiceException",
          "Lambda.AWSLambdaException",
          "Lambda.SdkClientException"
        ],
        "IntervalSeconds": 2,
        "MaxAttempts": 6,
        "BackoffRate": 2
      }
    ],
    "Catch": [
      {
        "ErrorEquals": [
          "States.ALL"
        ],
        "ResultPath": "$.exception",
        "Next": "WorkflowFailed"
      }
    ],
    "Next": "QueueGranulesLambdaBatch"
  }
}
```

# Batching
As of v2.0.0 this module now supports batching to the QueueGranules step. In order to take advantage of this, the 
Discover Granules workflow must be modified to include a post-QueueGranules step to check whether there are more 
granules to queue from the discovery process. It is important to note that batching cannot be used when using 
`db_type: "cumulus"`. The following is an example definition of the choice step. 

IsDone Definition:  
```json
{
  "IsDone": {
      "Type": "Choice",
      "Choices": [
        {
          "Or": [
            {
              "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
              "NumericEqualsPath": "$.meta.collection.meta.discover_tf.discovered_files_count"
            },
            {
              "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
              "NumericGreaterThanPath": "$.meta.collection.meta.discover_tf.discovered_files_count"
            },
            {
              "Variable": "$.meta.collection.meta.discover_tf.batch_size",
              "NumericEquals": 0
            }
          ],
          "Next": "WorkflowSucceeded"
        },
        {
          "Variable": "$.meta.collection.meta.discover_tf.batch_delay",
          "IsPresent": true,
          "Next": "WaitStep"
        },
        {
          "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
          "NumericLessThanPath": "$.meta.collection.meta.discover_tf.discovered_files_count",
          "Next": "GHRCDiscoverGranulesLambda"
        }
      ],
      "Default": "WorkflowFailed"
    }
}
```
WaitStep:
```json
{
  "WaitStep": {
    "Type": "Wait",
    "SecondsPath": "$.meta.collection.meta.discover_tf.batch_delay",
    "Next": "GHRCDiscoverGranulesLambda"
  }
}
```
The main difference between previous implementations and the batching functionality is that an attempt will be
made to discover all granules for a provider and writes this to the configured database. Once the discovery process is 
complete the module will fetch records from the database, limited by the batch_limit parameter, and generate the 
appropriate output for the QueueGranules step. The code internally keeps up with the number of discovered and queued 
granules and will keep looping between the `IsDone` step and `DiscoverGranules` step until all granules have been marked 
as queued in the database. 

# Skip Ingest
It is possible to skip the queue granules step and it can be convenient to do so for some situations. The following
is an example of skip step.

SkipStep:
```json
{
  "SkipQueueGranules": {
    "Type": "Choice",
    "Choices": [
      {
        "And": [
          {
            "Variable": "$.meta.collection.meta.discover_tf.skip_queue_granules",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.skip_queue_granules",
            "BooleanEquals": true
          }
        ],
        "Next": "IsDone"
      }
    ],
    "Default": "QueueGranulesLambdaBatch"
  }
}
```

# Building
There is a `build.sh` script for convenience that uses values from the host environment to build and deploy the lambda.
To use this, add the following environment variables:
```shell
export AWS_ACCOUNT_NUMBER=<ACCOUNT_NUMBER>
export AWS_REGION=<REGION>
export PREFIX=<STACK_PREFIX>
```
Once configured, a build and deployment can be done with `bash build.sh`

# Testing
There is a createPackage.py script located at the top level of the ghrc-discover-granules repo that can use used to
create a zip and then the dev stack repo can be pointed to this zip file. Change the source of the 
"ghrc-discover-granules" to point to the zip in your ghrc-discover-granules local repo.   
Alternatively, you can just directly deploy the updated lambda via the following AWS CLI command:
```
python createPackage.py && aws lambda update-function-code --function-name 
arn:aws:lambda:<region>:<account_number>:function:ghrcsbxw-ghrc-discover-granules-module --zip-file fileb://ghrc_discover_granules_lambda.zip
--publish
```
Notes:
 - You will need to update the --function-name to the appropriate value for the stack you are working in. 
You can download the database lookup file stored in S3, modify it for testing, and upload it as needed.
 - The ETag as given by AWS is used as the md5sum. AWS calculates the ETag for multipart downloads somewhat differently
than normal uploads. This probably won't matter in most cases as GDG just uses the Etag as a unique identifier. 
If a verification of the Etag needs to be done then look into how AWS S3 calculates a multipart file upload checksum.  
