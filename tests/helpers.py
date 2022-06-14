def get_event(provider, granuleId_extraction, provider_path, discover_tf):
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


def get_s3_event():
    return {
        'input': {},
        'config': {
            'provider': {
                'createdAt': 1634748290011,
                'id': 'private_bucket',
                'host': 'sharedsbx-private',
                'updatedAt': 1634748290011,
                'protocol': 's3'
            },
            'provider_path': 'lma/nalma/raw/short_test',
            'collection': {
                'process': 'metadataextractor',
                'granuleIdExtraction': '^((.*_NALMA_).*)',
                'version': '1',
                'meta': {
                    'discover_tf': {
                        'force_replace': 'true',
                        'depth': 0,
                        'dir_reg_ex': '.*'
                    },
                    'hyrax_processing': 'false',
                    'granuleRecoveryWorkflow': 'OrcaRecoveryWorkflow',
                    'provider_path': 'lma/nalma/raw/short_test',
                    'excludeFileTypes': [
                        '.*dat$'
                    ],
                    'collection_type': 'ongoing',
                    'metadata_extractor': [
                        {
                            'regex': '^.*_NALMA_(.*)(\\.dat)$',
                            'module': 'ascii'
                        }
                    ]
                },
                'createdAt': 1645808893670,
                'name': 'nalmaraw',
                'duplicateHandling': 'replace',
                'files': [
                    {
                        'bucket': 'public',
                        'regex': '^.*_NALMA_.*\\.cmr\\.(xml|json)$',
                        'reportToEms': True,
                        'sampleFileName': 'LK_NALMA_courtland_201015_035000.dat.gz.cmr.xml',
                        'lzards': {
                            'backup': False
                        }
                    },
                    {
                        'bucket': 'protected',
                        'regex': '^.*_NALMA_.*(\\.gz)$',
                        'reportToEms': True,
                        'sampleFileName': 'LK_NALMA_courtland_201015_035000.dat.gz',
                        'lzards': {
                            'backup': False
                        }
                    },
                    {
                        'bucket': 'private',
                        'regex': '^.*_NALMA_.*(\\.dat)$',
                        'reportToEms': False,
                        'sampleFileName': 'LK_NALMA_courtland_201015_035000.dat',
                        'lzards': {
                            'backup': False
                        }
                    }
                ],
                'updatedAt': 1646067370367,
                'url_path': 'nalmaraw__1',
                'reportToEms': True,
                'granuleId': '^.*_NALMA_.*(\\.dat)$',
                'sampleFileName': 'LK_NALMA_courtland_201015_035000.dat'
            },
            'buckets': {
                'dashboard': {
                    'name': 'sharedsbx-dashboard',
                    'type': 'public'
                },
                'internal': {
                    'name': 'sharedsbx-internal',
                    'type': 'internal'
                },
                'orca_default': {
                    'name': '',
                    'type': 'orca'
                },
                'private': {
                    'name': 'sharedsbx-private',
                    'type': 'private'
                },
                'protected': {
                    'name': 'sharedsbx-protected',
                    'type': 'protected'
                },
                'public': {
                    'name': 'sharedsbx-public',
                    'type': 'public'
                },
                'sharedprivate': {
                    'name': 'ghrcw-private',
                    'type': 'sharedprivate'
                }
            },
            'stack': 'sharedsbx',
            'duplicateGranuleHandling': 'replace'
        },
        'cumulus_config': {
            'state_machine': 'arn:aws:states:us-west-2:322322076095:stateMachine:sharedsbx-DiscoverGranules',
            'execution_name': 'dg-nalmaraw-7a7b991a-cc3f-45ce-af4f-40b9b3647ac2'
        }
    }

    pass
