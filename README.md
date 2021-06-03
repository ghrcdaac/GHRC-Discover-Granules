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
depth: How far you want the recursive search to go from the starting URL.  
depth = 3  
https://website.com/  
https://website.com/depth1  
https://website.com/depth1/depth2  
https://website.com/depth1/depth2/depth3  

Note: The absolute value will be taken of this parameter so negative values are not intended to be used for upward recursion.

file_reg_ex: Regular expression used to search for matching granules  
dir_reg_ex: Regular expression used to only search directories it matches

 
 This is an example of a collection with the added block:
 ```json
{	
 "name": "rssmif16d",
	"version": "7",
	"process": "rss",
	"url_path": "rssmif16d__7",
	"duplicateHandling": "skip",
	"granuleId": "^(f16_\\d{8}v7).*$",
	"granuleIdExtraction": "^(f16_\\d{8}v7.gz)$",
	"reportToEms": true,
	"sampleFileName": "f16_20190301v7.gz",
	"files": [
		{
			"bucket": "internal",
			"regex": "^(f16_.*)\\dv7.gz$",
			"sampleFileName": "f16_20190301v7.gz",
			"reportToEms": true
		},
		{
			"bucket": "protected",
			"regex": "^(f16_.*)\\.(dmrpp|nc)$",
			"sampleFileName": "f16_ssmis_20190316v7.nc",
			"reportToEms": true
		},
		{
			"bucket": "public",
			"regex": "^(f16_.*).nc\\.cmr\\.xml$",
			"sampleFileName": "f16_ssmis_20190316v7.nc.cmr.xml",
			"reportToEms": true
		}
	],
	"updatedAt": 1622663562684,
	"meta": {
		"discover_tf": {
			"depth": 2,
			"file_reg_ex": ".*",
			"dir_reg_ex": ".*"
		},
		"hyrax_processing": "false",
		"payload": [],
		"provider_path": "/ssmi/f16/bmaps_v07/y%YYYY/m%MM/",
		"collectionVersion": "7",
		"recursive_discover": "true",
		"collectionName": "rssmif16d",
		"metadata_extractor": [
			{
				"lon_var_key": "longitude",
				"regex": "^(f16_.*).nc$",
				"time_units": "units",
				"module": "netcdf",
				"time_var_key": "time",
				"lat_var_key": "latitude"
			}
		]
	}
}
```

The last relevant value in the collection definition is "duplicateHandling". Discovere granules handles 3 possible value for this:
 - skip: Only overrite the ETag and Last-Modified values pulled from S3 if they differ
 - replace: Erases the previous runs to force a clean collection run
 - error: If a granule is encountered that has been discovered before a ValueError exception will be thrown and execution will cease

# Testing
If code changes need to be made to the discover-granules-tf-module code it is advised to clone this repo and the dev stack repo http://gitlab.com/ghrc-cloud/ghrc-tf-deploy
There is a createPackage.py script located at the top level of the discover-granules-tf-module repo that can use used to create a zip and then the dev stack repo can be pointed to this zip file. To do this open ghrc-tf/lambdas.tf in the dev stack repo and change the source of the "discover-granules-tf-module" to point to the zip in your discover-granules-tf-module local repo.
