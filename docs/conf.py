from docs_conf.conf import *

#branch configuration

branch = 'f-release'

language = 'en'

linkcheck_ignore = [
    'http://localhost.*',
    'http://127.0.0.1.*',
    'https://gerrit.o-ran-sc.org.*',
    './dmaap-adapter-api.html', #Generated file that doesn't exist at link check.
]

extensions = ['sphinxcontrib.redoc', 'sphinx.ext.intersphinx',]

redoc = [
            {
                'name': 'DMaaP Adapter API',
                'page': 'dmaap-adapter-api',
                'spec': '../api/api.json',
            }
        ]

redoc_uri = 'https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js'

#intershpinx mapping with other projects
intersphinx_mapping = {}

intersphinx_mapping['nonrtric'] = ('https://docs.o-ran-sc.org/projects/o-ran-sc-nonrtric/en/%s' % branch, None)
