TIME_CORRECTION = 3  # делаем поправку на время по москве
PATH_FOR_WIKIPAGEVIEWS_GZ = "/data/wikipageviews_gz"
PATH_WORK_FILES = "/data/work_files"
PASTGRES_CONN_ID = "wiki_views_postgres"
CLICKHOUSE_CONN_ID = "wiki_views_clickhouse"
DOMAIN_CONFIG = {
    "ru": {
        "domain_code": "ru",
        "time_correction": 3,
        "resourse": "rus_Cyrl",
        "translate_other_languages": True,
    },  # русский
    "en": {
        "domain_code": "en",
        "time_correction": -4,
        "resourse": "eng_Latn",
        "translate_other_languages": True,
    },  # английский
    "es": {
        "domain_code": "es",
        "time_correction": 2,
        "resourse": "spa_Latn",
        "translate_other_languages": False,
    },  # испанский
    "pt": {
        "domain_code": "pt",
        "time_correction": -3,
        "resourse": "por_Latn",
        "translate_other_languages": False,
    },  # португальский
    "fr": {
        "domain_code": "fr",
        "time_correction": 2,
        "resourse": "fra_Latn",
        "translate_other_languages": False,
    },  # французкий
    "de": {
        "domain_code": "de",
        "time_correction": 2,
        "resourse": "deu_Latn",
        "translate_other_languages": False,
    },  # немецкий
    "it": {
        "domain_code": "it",
        "time_correction": 2,
        "resourse": "ita_Latn",
        "translate_other_languages": False,
    },  # итальянский
    "uk": {
        "domain_code": "uk",
        "time_correction": 3,
        "resourse": "ukr_Cyrl",
        "translate_other_languages": False,
    },  # украинский
    "tr": {
        "domain_code": "tr",
        "time_correction": 3,
        "resourse": "tur_Latn",
        "translate_other_languages": False,
    },  # турецкий
    "fa": {
        "domain_code": "fa",
        "time_correction": 3,
        "resourse": "pes_Arab",
        "translate_other_languages": False,
    },  # французкий
    "pl": {
        "domain_code": "pl",
        "time_correction": 2,
        "resourse": "pol_Latn",
        "translate_other_languages": False,
    },  # польский
    "arz": {
        "domain_code": "arz",
        "time_correction": 3,
        "resourse": "arz_Arab",
        "translate_other_languages": False,
    },  # арабский египетский
    "ar": {
        "domain_code": "ar",
        "time_correction": 3,
        "resourse": "ar",
        "translate_other_languages": False,
    },  # арабский
    "hi": {
        "domain_code": "hi",
        "time_correction": 5,
        "resourse": "hin_Deva",
        "translate_other_languages": True,
    },  # хинди
    "ja": {
        "domain_code": "ja",
        "time_correction": 9,
        "resourse": "jpn_Jpan",
        "translate_other_languages": False,
    },  # японский
    "id": {
        "domain_code": "id",
        "time_correction": 7,
        "resourse": "ind_Latn",
        "translate_other_languages": False,
    },  # индонезия
    "he": {
        "domain_code": "he",
        "time_correction": 3,
        "resourse": "heb_Hebr",
        "translate_other_languages": False,
    },  # иврит
    "hy": {
        "domain_code": "hy",
        "time_correction": 4,
        "resourse": "hye_Armn",
        "translate_other_languages": False,
    },  # армянский
    "az": {
        "domain_code": "az",
        "time_correction": 4,
        "resourse": "azj_Latn",
        "translate_other_languages": False,
    },  # азербанжанский
    "sr": {
        "domain_code": "sr",
        "time_correction": 2,
        "resourse": "srp_Cyrl",
        "translate_other_languages": False,
    },  # сербский
}

target = [
    value["resourse"]
    for value in DOMAIN_CONFIG.values()
    if value["translate_other_languages"]
]
for key, value in DOMAIN_CONFIG.items():
    copy_target = target.copy()
    if value["resourse"] in copy_target:
        copy_target.remove(value["resourse"])
    DOMAIN_CONFIG[key]["target"] = copy_target
