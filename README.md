[![Coverage Status](https://coveralls.io/repos/github/ghrcdaac/discover-granules-tf-module/badge.svg)](https://coveralls.io/github/ghrcdaac/discover-granules-tf-module)
![Build Status](https://github.com/amarouane-ABDELHAK/discover-granules-tf-module/actions/workflows/python-package.yml/badge.svg?branch=master)
![Code Quality Workflow](https://github.com/amarouane-ABDELHAK/discover-granules-tf-module/actions/workflows/python-package.yml/badge.svg?branch=master)
![Code Quality](https://api.codiga.io/project/33434/score/svg)
![Code Grade](https://api.codiga.io/project/33434/status/svg)
```
 ____ ___ ____   ____ _____     _______ ____        ____ ____      _    _   _ _   _ _     _____ ____      _____ _____
|  _ \_ _/ ___| / ___/ _ \ \   / / ____|  _ \      / ___|  _ \    / \  | \ | | | | | |   | ____/ ___|    |_   _|  ___|
| | | | |\___ \| |  | | | \ \ / /|  _| | |_) |____| |  _| |_) |  / _ \ |  \| | | | | |   |  _| \___ \ _____| | | |_
| |_| | | ___) | |__| |_| |\ V / | |___|  _ <_____| |_| |  _ <  / ___ \| |\  | |_| | |___| |___ ___) |_____| | |  _|
|____/___|____/ \____\___/  \_/  |_____|_| \_\     \____|_| \_\/_/   \_\_| \_|\___/|_____|_____|____/      |_| |_|
```
# Overview
The discover granules terraform module uses a lambda function to recursively discover files provided via HTTP/HTTPS
and S3 protocols. 
The code retrieves the granule names, ETag and Last-Modified values from the provider and stores the results as a sqlite
database file in S3.  
ETag: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag  
Last-Modified: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified  

## Supported Protocols and Limitations
Currently granules can be discovered via AWS S3, HTTP/HTTPS, and SFTP protocols. There are some limitations such as 
HTTP/HTTPS not supporting redirects or username and passwords. Additionally, SFTP does not support public/private key
authentication.

## Versioning
We are following `v<major>.<minor>.<patch>` versioning convention, where:
* `<major>+1` means we changed the infrastructure and/or the major components that makes this software run. Will definitely 
  lead to breaking changes.
* `<minor>+1` means we upgraded/patched the dependencies this software relays on. Can lead to breaking changes.
* `<patch>+1` means we fixed a bug and/or added a feature. Breaking changes are not expected.

# 🔨 Pre-requisite 
This module is meant to run within Cumulus stack. 
If you don't have Cumulus stack deployed yet please consult [this repo](https://github.com/nasa/cumulus) 
and follow the [documetation](https://nasa.github.io/cumulus/docs/cumulus-docs-readme) to provision it.

The dev stack repo is also needed to deploy and test changes to discover-granules-tf-module: https://gitlab.com/ghrc-cloud/ghrc-tf-deploy

# How to
In order to use the recursive discover granules the following block must be added to the collection definition inside 
of the meta block:
```json
"discover_tf": {
 "depth": 0,
 "force_replace": "false",
 "dir_reg_ex": ".*"
}
```
Collection definitions can be found in this repo: https://gitlab.com/ghrc-cloud/ghrc-tf-configuration/-/tree/master/collections  

depth: How far you want the recursive search to go from the starting URL. The search will look for granules in each level
and traverse directories until there are no directories or depth is reached. This value is only applicable to http/https
providers.  
Note: The absolute value will be taken of this parameter so negative values are not intended to be used for upward traversal.

force_replace: This can be used to force Discover Granules to rediscover all granules even if previously discovered.
The duplicateHandling flag being set to replace defaults to "skip" to handle reingesting previously discovered files that 
have been updated.

dir_reg_ex: Regular expression used to only search directories it matches

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

# Results
The results of a successful run will be stored in S3 as a sqlite database file. The bucket is currently 
&lt;prefix&gt;-internal/discover-granule/lookup. The location is set in the ghrc-tf/lambdas file in the dev stack repo. 
The name of the file will be discover_granules.db.  
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
      "bucket": "ghrcsbxw-internal",
      "url_path": "rssmif16d__7",
      "type": ""
    }
  ]
}
```
Note: The actual output uses single quotes but double quotes were used here to avoid syntax error highlighting.  

# Testing
There is a createPackage.py script located at the top level of the discover-granules-tf-module repo that can use used to
create a zip and then the dev stack repo can be pointed to this zip file. To do this open ghrc-tf/lambdas.tf in the dev 
stack repo and change the source of the "discover-granules-tf-module" to point to the zip in your 
discover-granules-tf-module local repo.   
Alternatively you can just directly deploy the updated lambda via the following AWS CLI command:
```commandline
python createPackage.py && aws lambda update-function-code --function-name 
arn:aws:lambda:us-west-2:123456789101:function:ghrcsbxw-discover-granules-tf-module --zip-file fileb://package.zip 
--publish
```
Notes:
 - You will need to update the --function-name to the appropriate value for the stack you are working in. 
You can download the database lookup file stored in S3, modify it for testing, and upload it as needed.
 - The ETag as given by AWS is used as the md5sum. AWS calculates the ETag for multipart downloads slightly different
than normal uploads. This probably won't matter in most cases as RDG just uses the Etag as a unique identifier. 
If a verification of the Etag needs to be done then look into how AWS S3 calculates a multipart file upload checksum.  
