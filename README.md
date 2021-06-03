```
 ____ ___ ____   ____ _____     _______ ____        ____ ____      _    _   _ _   _ _     _____ ____      _____ _____
|  _ \_ _/ ___| / ___/ _ \ \   / / ____|  _ \      / ___|  _ \    / \  | \ | | | | | |   | ____/ ___|    |_   _|  ___|
| | | | |\___ \| |  | | | \ \ / /|  _| | |_) |____| |  _| |_) |  / _ \ |  \| | | | | |   |  _| \___ \ _____| | | |_
| |_| | | ___) | |__| |_| |\ V / | |___|  _ <_____| |_| |  _ <  / ___ \| |\  | |_| | |___| |___ ___) |_____| | |  _|
|____/___|____/ \____\___/  \_/  |_____|_| \_\     \____|_| \_\/_/   \_\_| \_|\___/|_____|_____|____/      |_| |_|
```

# Overview
A discover granules terraform module uses a lambda function to recursively discover files provided via protocol X

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

# How to
In order to use the recursive discover granules the following block must be added to the collection definition inside of the meta block:
```json
"discover_tf": {
 "depth": 0,
 "file_reg_ex": ".*",
 "dir_reg_ex": ".*"
}
```

Collection definitions can be found in this repo: https://gitlab.com/ghrc-cloud/ghrc-tf-configuration/-/tree/master/collections  

depth: How far you want the recursive search to go from the starting URL. The search will look for granules in each level and traverse directories until there are no directories or depth is reached.  
Note: The absolute value will be taken of this parameter so negative values are not intended to be used for upward traversal.

file_reg_ex: Regular expression used to search for matching granules  
dir_reg_ex: Regular expression used to only search directories it matches

 
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
            "file_reg_ex": ".*", 
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

The last relevant value in the collection definition is "duplicateHandling". Discovere granules handles 3 possible value for this:
 - skip: Only overrite the ETag and Last-Modified values pulled from S3 if they differ
 - replace: Erases the previous runs to force a clean collection run
 - error: If a granule is encountered that has been discovered before a ValueError exception will be thrown and execution will cease

# Results
The results of a successful run will be stored in S3. The bucket is currently ghrcsbxw-internal/discover-granule/lookup. The location is set in the ghrc-tf/lambdas file in the dev stack repo. The name of the file will be collection_name__version.csv

# Testing
If code changes need to be made to the discover-granules-tf-module code it is advised to clone this repo and the dev stack repo http://gitlab.com/ghrc-cloud/ghrc-tf-deploy
There is a createPackage.py script located at the top level of the discover-granules-tf-module repo that can use used to create a zip and then the dev stack repo can be pointed to this zip file. To do this open ghrc-tf/lambdas.tf in the dev stack repo and change the source of the "discover-granules-tf-module" to point to the zip in your discover-granules-tf-module local repo.
