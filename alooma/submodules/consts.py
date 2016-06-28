# Code Engine
EVENT_DROPPING_TRANSFORM_CODE = 'def transform(event):\n\treturn None'
DEFAULT_TRANSFORM_CODE = "def transform(event):\n\treturn event"

# Configurations
DEFAULT_SETTINGS_EMAIL_NOTIFICATIONS = {
    "digestInfo": True,
    "digestWarning": True,
    "digestError": True,
    "digestFrequency": "DAILY",
    "recipientsChanged": False,
    "recipients": []
}


# Inputs
POST_DATA_CONFIGURATION = 'configuration'
POST_DATA_NAME = 'name'
POST_DATA_TYPE = 'type'
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


# Mapper
MAPPING_MODES = ['AUTO_MAP', 'STRICT', 'FLEXIBLE']


# Metrics
METRICS_LIST = [
    'EVENT_SIZE_AVG',
    'EVENT_SIZE_TOTAL',
    'EVENT_PROCESSING_RATE',
    'CPU_USAGE',
    'MEMORY_CONSUMED',
    'MEMORY_LEFT',
    'INCOMING_EVENTS',
    'RESTREAMED_EVENTS',
    'UNMAPPED_EVENTS',
    'IGNORED_EVENTS',
    'ERROR_EVENTS',
    'LOADED_EVENTS_RATE',
    'LATENCY_AVG',
    'LATENCY_PERCENTILE_50',
    'LATENCY_PERCENTILE_95',
    'LATENCY_MAX',
    'EVENTS_IN_PIPELINE',
    'EVENTS_IN_TRANSIT'
]


# Redshift
RESTREAM_QUEUE_TYPE_NAME = "RESTREAM"
REDSHIFT_TYPE = "REDSHIFT"


# Alooma
DEFAULT_ENCODING = 'utf-8'
