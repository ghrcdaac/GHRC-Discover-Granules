[![Coverage Status](https://coveralls.io/repos/github/ghrcdaac/discover-granules-tf-module/badge.svg)](https://coveralls.io/github/ghrcdaac/discover-granules-tf-module)
![Build Status](https://github.com/ghrcdaac/discover-granules-tf-module/actions/workflows/python-package.yml/badge.svg?branch=master)
![Code Quality Workflow](https://github.com/ghrcdaac/discover-granules-tf-module/actions/workflows/code-quality.yml/badge.svg?branch=master)
![Code Quality](https://api.codiga.io/project/33591/score/svg)
![Code Grade](https://api.codiga.io/project/33591/status/svg)
```
 ____ ___ ____   ____ _____     _______ ____        ____ ____      _    _   _ _   _ _     _____ ____      _____ _____
|  _ \_ _/ ___| / ___/ _ \ \   / / ____|  _ \      / ___|  _ \    / \  | \ | | | | | |   | ____/ ___|    |_   _|  ___|
| | | | |\___ \| |  | | | \ \ / /|  _| | |_) |____| |  _| |_) |  / _ \ |  \| | | | | |   |  _| \___ \ _____| | | |_
| |_| | | ___) | |__| |_| |\ V / | |___|  _ <_____| |_| |  _ <  / ___ \| |\  | |_| | |___| |___ ___) |_____| | |  _|
|____/___|____/ \____\___/  \_/  |_____|_| \_\     \____|_| \_\/_/   \_\_| \_|\___/|_____|_____|____/      |_| |_|
```
# Overview
The discover granules terraform module uses a lambda function to recursively discover files provided via HTTP/HTTPS, SFTP
and S3 protocols. 
The code retrieves the granule names, ETag and Last-Modified values from the provider and stores the results as a SQLite
database file in an EFS mount point that is retrieved from the `efs_path` environment variable. If this is not provided 
the database will be written to a temporary directory which is not persistent. 
ETag: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag  
Last-Modified: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified  

## Supported Protocols
 - AWS S3, HTTP/HTTPS, and SFTP 
## Limitations
 - HTTP/HTTPS does not support redirects or username + password authentication

## Versioning
We are following `v<major>.<minor>.<patch>` versioning convention, where:
* `<major>+1` means we changed the infrastructure and/or the major components that makes this software run. Will definitely 
  lead to breaking changes.
* `<minor>+1` means we upgraded/patched the dependencies this software relays on. Can lead to breaking changes.
* `<patch>+1` means we fixed a bug and/or added a feature. Breaking changes are not expected.

# 🔨 Pre-requisite 
## Cumulus
This module is meant to run within Cumulus stack. 
If you don't have Cumulus stack deployed yet please consult [this repo](https://github.com/nasa/cumulus) 
and follow the [documentation](https://nasa.github.io/cumulus/docs/cumulus-docs-readme) to provision it.  
## EFS
It is recommended to set up an EFS partition in EC2 to store the SQLite database. Persistent memory will not be possible
without doing this but would still be possible to run one off discovery processes. See the following repo for 
setting it up: 
https://github.com/ghrcdaac/terraform-aws-efs-mount/releases/download/v0.1.4/terraform-aws-efs-mount.zip

# How to
In order to use the recursive discover granules the following block must be added to the collection definition inside 
of the meta block:
```json
"discover_tf": {
 "depth": 0,
 "force_replace": "false",
 "dir_reg_ex": "",
 "file_reg_ex": "",
 "batch_limit": 1000
}
``` 

 - `depth`: How far you want the recursive search to go from the starting URL. The search will look for granules in each level
and traverse directories until there are no directories or depth is reached. This value is only applicable to http/https
providers.  
Note: The absolute value will be taken of this parameter so negative values are not intended to be used for upward traversal.

 - `force_replace`: This can be used to force Discover Granules to rediscover all granules even if previously discovered.
The duplicateHandling flag being set to replace defaults to "skip" to handle reingesting previously discovered files that 
have been updated.

 - `dir_reg_ex`: Regular expression used to only search directories it matches

 - `file_reg_ex`: Regular expression used to only discover files it matches

 - `batch_limit`: Used to specify the size of the batches sent to QueueGranules when using the IsDone step in the DIscoverGranules workflow. Will default to 1000 if not provided. If you do not want to use batching provide a number larger than the expect number of granules to discover. This will effectively prevent batching. Note, this could cause memory issues for the DiscoverGranules or QueueGranules lambdas. 

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
            "force_replace": "false",
            "dir_reg_ex": ".*" 
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
discover-granules-tf-module how to handle granules that exist in the sqlite database file but also are discovered on the
current run. Discover granules handles 3 possible value for this:
 - skip: Overwrite the ETag or Last-Modified values pulled from S3 if they differ from what the provider returns for 
   this run
 - replace: The results for this collection that are currently stored in the S3 database file will be overwritten with 
   the results of this run
 - error: If a granule is encountered that has been discovered before a ValueError exception will be thrown and 
   execution will cease 
   
# Step Configuration
The following definition is an example of defining the lambda as a step in a AWS statemachine. This configuration is read by the CMA which can be read about here: https://nasa.github.io/cumulus/docs/workflows/input_output
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
    "Resource": "${discover_granules_tf_arn}",
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

# Output
The module generates output that should match the output for the Cumulus DiscoverGranules 
lambda: https://github.com/nasa/cumulus/tree/master/tasks/discover-granules   
Here is a sample excerpt from the database:  

http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210501v7.gz,"e636b16d603fd71:0",2021-05-02 14:35:42+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210501v7_d3d.gz,"bf74b470603fd71:0",2021-05-02 14:35:47+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210502v7.gz,"4d338a972940d71:0",2021-05-03 14:35:41+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210502v7_d3d.gz,"c6f29b982940d71:0",2021-05-03 14:35:42+00:00  
http://data.remss.com/ssmi/f16/bmaps_v07/y2021/m05/f16_20210503v7.gz,"65f3f5cff040d71:0",2021-05-04 14:21:45+00:00  

The step function returns a dictionary of granules that were discovered this run. This is an example of one of the dictionary entries:   
```json
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
```
Note: The actual output uses single quotes but double quotes were used here to avoid syntax error highlighting.

# Batching
As of v2.0.0 this module now supportes batching to the QueueGranules step. In order to take advantage of this the discover granules workflow must be modified to include a post-QueueGranules step to check whether there are more granules to queue from the discover process. The following is an example definition of the choice step. 

```json
{
  "IsDone": {
    "Type": "Choice",
    "Choices": [
      {
        "And": [
          {
            "Variable": "$.meta.collection.meta.discover_tf.discovered_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "NumericEqualsPath": "$.meta.collection.meta.discover_tf.discovered_files_count"
          }
        ],
        "Next": "WorkflowSucceeded"
      },
      {
        "And": [
          {
            "Variable": "$.meta.collection.meta.discover_tf.discovered_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.batch_delay",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "NumericLessThanPath": "$.meta.collection.meta.discover_tf.discovered_files_count"
          }
        ],
        "Next": "WaitStep"
      },
      {
        "And": [
          {
            "Variable": "$.meta.collection.meta.discover_tf.discovered_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.batch_delay",
            "IsPresent": false
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "NumericLessThanPath": "$.meta.collection.meta.discover_tf.discovered_files_count"
          }
        ],
        "Next": "GHRCDiscoverGranulesLambda"
      },
      {
        "And": [
          {
            "Variable": "$.meta.collection.meta.discover_tf.discovered_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "IsPresent": true
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "NumericGreaterThanPath": "$.meta.collection.meta.discover_tf.discovered_files_count"
          }
        ],
        "Next": "WorkflowFailed"
      },
      {
        "And": [
          {
            "Variable": "$.meta.collection.meta.discover_tf.discovered_files_count",
            "IsPresent": false
          },
          {
            "Variable": "$.meta.collection.meta.discover_tf.queued_files_count",
            "IsPresent": false
          }
        ],
        "Next": "WorkflowSucceeded"
      }
    ]
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
made to discover all granules for a provider and writes this to the SQLite database. Once the discovery process is 
complete the module will fetch records from the database, limited by the batch_limit parameter, and generate the 
appropriate output for the QueueGranules step. The code internally keeps up with the number of discovered and queued 
granules and will keep looping between the `IsDone` step and `DiscoverGranules` step until all granules have been marked 
as queued in the SQLite database. 

# Testing
There is a createPackage.py script located at the top level of the discover-granules-tf-module repo that can use used to
create a zip and then the dev stack repo can be pointed to this zip file. Change the source of the 
"discover-granules-tf-module" to point to the zip in your discover-granules-tf-module local repo.   
Alternatively you can just directly deploy the updated lambda via the following AWS CLI command:
```commandline
python createPackage.py && aws lambda update-function-code --function-name 
arn:aws:lambda:<region>:<account_number>:function:ghrcsbxw-discover-granules-tf-module --zip-file fileb://package.zip 
--publish
```
Notes:
 - You will need to update the --function-name to the appropriate value for the stack you are working in. 
You can download the database lookup file stored in S3, modify it for testing, and upload it as needed.
 - The ETag as given by AWS is used as the md5sum. AWS calculates the ETag for multipart downloads slightly different
than normal uploads. This probably won't matter in most cases as RDG just uses the Etag as a unique identifier. 
If a verification of the Etag needs to be done then look into how AWS S3 calculates a multipart file upload checksum.  
