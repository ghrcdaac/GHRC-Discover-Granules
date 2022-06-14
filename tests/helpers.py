import json
import os


def get_event(event_protocol):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{dir_path}/event_{event_protocol}.json', 'r') as file:
        json_dict = json.load(file)

    return json_dict


def configure_event(provider, granuleId_extraction, provider_path, discover_tf):
    """
    returns event data
    """
    return {
        "input": {},
        "config": {
            "provider": provider,
            "provider_path": provider_path,
            "collection": {
                "process": "rss",
                "granuleIdExtraction": granuleId_extraction,
                "version": "7",
                "dataType": "rssmif16d",
                "meta": {
                    "discover_tf": discover_tf,
                    "granuleRecoveryWorkflow": "DrRecoveryWorkflow",
                    "hyrax_processing": "true",
                    "provider_path": provider_path,
                    "collectionVersion": "7",
                    "collection_type": "test_collection",
                    "collectionName": "rssmif16d",
                    "metadata_extractor": [
                        {
                            "lon_var_key": "longitude",
                            "regex": "^(f16_.*).nc$",
                            "time_units": "units",
                            "time_var_key": "time",
                            "lat_var_key": "latitude",
                            "module": "netcdf"
                        }
                    ],
                    "workflow_name": "LZARDSBackup"
                },
                "createdAt": 1624396693367,
                "name": "rssmif16d",
                "duplicateHandling": "skip",
                "files": [
                    {
                        "bucket": "public",
                        "regex": "^.*_NALMA_.*\\.cmr\\.(xml|json)$",
                        "sampleFileName": "LK_NALMA_courtland_201015_035000.dat.gz.cmr.xml",
                        "reportToEms": "True"
                    },
                    {
                        "bucket": "protected",
                        "regex": "^.*_NALMA_.*(\\.gz)$",
                        "sampleFileName": "LK_NALMA_courtland_201015_035000.dat.gz",
                        "reportToEms": "True"
                    },
                    {
                        "bucket": "private",
                        "regex": "^.*_NALMA_.*(\\.dat)$",
                        "sampleFileName": "LK_NALMA_courtland_201015_035000.dat",
                        "reportToEms": "True"
                    }
                ]
                ,
                "updatedAt": 1628711282117,
                "url_path": "rss/rssmif16d__7",
                "reportToEms": "True",
                "granuleId": "^(f16_\\d{8}v7).*$",
                "sampleFileName": "f16_20190301v7.gz"
            },
            "buckets": {
                "dashboard": {
                    "name": "ghrcsbxw-dashboard",
                    "type": "public"
                },
                "glacier": {
                    "name": "ghrcsbxw-orca-glacier-archive",
                    "type": "orca"
                },
                "internal": {
                    "name": "ghrcsbxw-internal",
                    "type": "internal"
                },
                "private": {
                    "name": "ghrcsbxw-private",
                    "type": "private"
                },
                "protected": {
                    "name": "ghrcsbxw-protected",
                    "type": "protected"
                },
                "public": {
                    "name": "ghrcsbxw-public",
                    "type": "public"
                },
                "sharedprivate": {
                    "name": "ghrcw-private",
                    "type": "private"
                }
            },
            "stack": "ghrcsbxw",
            "duplicateGranuleHandling": "replace"
        },
        "cumulus_config": {
            "state_machine": "arn:aws:states:us-west-2:123456789101:stateMachine:ghrcsbxw-DiscoverGranules",
            "execution_name": "rssmif16d-791e4ebb-ad97-44f5-af56-e6047c2a2739"
        }
    }
