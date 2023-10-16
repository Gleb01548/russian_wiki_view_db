TIME_CORRECTION = 3  # делаем поправку на время по москве
PATH_FOR_WIKIPAGEVIEWS_GZ = "/data/wikipageviews_gz"
PATH_WORK_FILES = "/data/work_files"
PASTGRES_CONN_ID = "wiki_views_postgres"
CLICKHOUSE_CONN_ID = "wiki_views_clickhouse"
DOMAIN_CONFIG = {
    "ru": {
        "domain_code": "ru",
        "dag": None,
        "time_correction": 3,
    },
    "en": {
        "domain_code": "en",
        "dag": None,
        "time_correction": -4,
    },
}
