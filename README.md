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
 "dir_reg_ex": ".*"
}
```  
depth: How far you want the recursive search to go from the starting URL. The search will look for granules in each level and traverse directories until there are no directories or depth is reached. A depth value of 0 will not explore any discovered directories.  
Note: The absolute value will be taken of this parameter so negative values are not intended to be used for upward traversal.

dir_reg_ex: Regular expression used to only search directories it matches

In order to do pattern matching against granules the granuleIdExtraction field must be given the desired regular expression. 
 This is an example of a collection with the added block and granuleIdExtraction using a regular expression:
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
Collection definitions can be found in this repo: https://gitlab.com/ghrc-cloud/ghrc-tf-configuration/-/tree/master/collections   

The last relevant value in the collection definition is "duplicateHandling". After each successful run an output file is created in S3 with the results of the run. So for each subsequent run there are 3 options to tell the code what to do about duplicate values. The 3 possible value for this are:
 - skip: If a granule is discovered that we have discovered before, overrite the ETag and Last-Modified values pulled from S3 if they differ
 - replace: The values for the previous discoveries will be erased and replaced with the results of the current run
 - error: If a granule is encountered that has been discovered before a ValueError exception will be thrown by discover-granules-tf-module and execution will cease

# Results
The results of a successful run will be stored in S3. The bucket is currently ghrcsbxw-internal/discover-granule/lookup. The location is set in the ghrc-tf/lambdas file in the dev stack repo. The name of the file will be collection_name__version.csv.
The format of the csv is 3 columns that contain the granule's full path, ETag, and Last-Modified values. The ETag and Last-Modified are retrievied from the providers head response. requests.head(url).headers will contain both of these. If any changes are made to the file on the provider's server the ETag will be changed as well as the Last-Modified value. 

# Testing
If code changes need to be made to the discover-granules-tf-module code it is advised to clone this repo and the dev stack repo http://gitlab.com/ghrc-cloud/ghrc-tf-deploy
There is a createPackage.py script located at the top level of the discover-granules-tf-module repo that can use used to create a zip and then the dev stack repo can be pointed to this zip file. To do this open ghrc-tf/lambdas.tf in the dev stack repo and change the source of the "discover-granules-tf-module" to point to the zip in your discover-granules-tf-module local repo.  
It is possible to modify the csv and reupload it to S3. It is best to do this using notepad or a basic text editor to prevent extraneous newline characters from being add in the file. Modifying it in Excel has caused this to happen.

