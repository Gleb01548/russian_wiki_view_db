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
        "send_massages": True,
        "tags": ["#russian", "#ru"],
        "wikipedia_segment": "🇷🇺🇷🇺🇷🇺Russian🇷🇺🇷🇺🇷🇺",
        "message_settings": {
            "numerical_characteristic": {
                "day": "тыс.",
                "week": "млн.",
                "month": "млн.",
                "year": "млн.",
            },
            "date_period_type_translate": {
                "day": "день",
                "week": "неделя",
                "month": "месяц",
                "year": "год",
            },
            "day_of_week_translate": [
                "Воскресенье",
                "Понедельник",
                "Вторник",
                "Средний",
                "Четверг",
                "Пятница",
                "Суббота",
            ],
            "icon": "Индикатор | ",
            "rank_now": "Тек. рейтинг | ",
            "rank_last": "Прош. рейтинг | ",
            "page_name": "Имя страницы | ",
            "page_name_translate": "Перевод имени | ",
            "sum_views": "Кол. просмотров | ",
            "increment_percent": "Рост (в %) | ",
            "message_top_now": (
                "{{ wikipedia_segment }}\n"  # noqa
                "Информация за {{ date_period_type }} {{ ds }} ({{ day_of_week }})"  # noqa
                "\nОбщее число просмотров википедии: {{ count_views }} {{ numerical_characteristic }} "  # noqa
                "(прирост: {{ views_increment_percent }}% )\n"  # noqa
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"  # noqa
            ),  # noqa
            "message_new_pages": (
                "{{ wikipedia_segment }}\n"
                "Разница с прошедшим периодом ({{ date_period_type }}).\n"
                "\n"
                "Новые страницы в топе:\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_go_out_pages": (
                "{{ wikipedia_segment }}\n"
                "Страницы которые вышли из топа:\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_difference_pages": (
                "{{ wikipedia_segment }}\n"
                "Изминения среди тех страниц, которые в топе остались:\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),
        },
    },  # русский
    "en": {
        "domain_code": "en",
        "time_correction": -4,
        "resourse": "eng_Latn",
        "translate_other_languages": True,
        "send_massages": False,
        "tags": ["#english", "#en"],
        "wikipedia_segment": "🇺🇸🇬🇧English🇬🇧🇺🇸",
    },  # английский
    "es": {
        "domain_code": "es",
        "time_correction": 2,
        "resourse": "spa_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#spanish", "#es"],
        "wikipedia_segment": "🇪🇸🇲🇽🇦🇷🇻🇪Spanish🇨🇴🇨🇺🇺🇾",
    },  # испанский
    "pt": {
        "domain_code": "pt",
        "time_correction": -3,
        "resourse": "por_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#portuguese", "#pt"],
        "wikipedia_segment": "🇧🇷🇵🇹🇦🇴Portuguese🇲🇿🇨🇻🇬🇼",
    },  # португальский
    "fr": {
        "domain_code": "fr",
        "time_correction": 2,
        "resourse": "fra_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#french", "#fr"],
        "wikipedia_segment": "🇫🇷French🇫🇷",
    },  # французкий
    "de": {
        "domain_code": "de",
        "time_correction": 2,
        "resourse": "deu_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#german", "#de"],
        "wikipedia_segment": "🇩🇪German🇩🇪",
    },  # немецкий
    "it": {
        "domain_code": "it",
        "time_correction": 2,
        "resourse": "ita_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#italian", "#it"],
        "wikipedia_segment": "🇮🇹Italian🇮🇹",
    },  # итальянский
    "uk": {
        "domain_code": "uk",
        "time_correction": 3,
        "resourse": "ukr_Cyrl",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#ukrainian", "#uk"],
        "wikipedia_segment": "🇺🇦Ukrainia🇺🇦",
    },  # украинский
    "tr": {
        "domain_code": "tr",
        "time_correction": 3,
        "resourse": "tur_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#turkish", "#tr"],
        "wikipedia_segment": "🇹🇷Turkish🇹🇷",
    },  # турецкий
    "fa": {
        "domain_code": "fa",
        "time_correction": 3,
        "resourse": "pes_Arab",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#persian", "#fa"],
        "wikipedia_segment": "🇮🇷Persian🇮🇷",
    },  # французкий
    "pl": {
        "domain_code": "pl",
        "time_correction": 2,
        "resourse": "pol_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#polish", "#pl"],
        "wikipedia_segment": "🇵🇱Polish🇵🇱",
    },  # польский
    "arz": {
        "domain_code": "arz",
        "time_correction": 3,
        "resourse": "arz_Arab",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#egyptian_arabic", "#arz"],
        "wikipedia_segment": "🇪🇬EgyptianArabic🇪🇬",
    },  # арабский египетский
    "ar": {
        "domain_code": "ar",
        "time_correction": 3,
        "resourse": "arb_Arab",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#arabic", "#ar"],
        "wikipedia_segment": "🇮🇶Arabic🇮🇶",
    },  # арабский
    "hi": {
        "domain_code": "hi",
        "time_correction": 5,
        "resourse": "hin_Deva",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#hindi", "#hi"],
        "wikipedia_segment": "🇮🇳Hindi🇮🇳",
    },  # хинди
    "ja": {
        "domain_code": "ja",
        "time_correction": 9,
        "resourse": "jpn_Jpan",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#japanese", "#ja"],
        "wikipedia_segment": "🇯🇵Japanese🇯🇵",
    },  # японский
    "id": {
        "domain_code": "id",
        "time_correction": 7,
        "resourse": "ind_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#indonesian", "#id"],
        "wikipedia_segment": "🇮🇩Indonesian🇮🇩",
    },  # индонезия
    "he": {
        "domain_code": "he",
        "time_correction": 3,
        "resourse": "heb_Hebr",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#hebrew", "#he"],
        "wikipedia_segment": "🇮🇱Hebrew🇮🇱",
    },  # иврит
    "hy": {
        "domain_code": "hy",
        "time_correction": 4,
        "resourse": "hye_Armn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#armenian", "#hy"],
        "wikipedia_segment": "🇦🇲Armenian🇦🇲",
    },  # армянский
    "az": {
        "domain_code": "az",
        "time_correction": 4,
        "resourse": "azj_Latn",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#azerbanian", "#az"],
        "wikipedia_segment": "🇦🇿Azerbanian🇦🇿",
    },  # азербанжанский
    "sr": {
        "domain_code": "sr",
        "time_correction": 2,
        "resourse": "srp_Cyrl",
        "translate_other_languages": False,
        "send_massages": False,
        "tags": ["#serbian", "#sr"],
        "wikipedia_segment": "🇷🇸Serbian🇷🇸",
    },  # сербский
}

target = [
    (value["resourse"], value["domain_code"])
    for value in DOMAIN_CONFIG.values()
    if value["translate_other_languages"]
]

for key, value in DOMAIN_CONFIG.items():
    copy_target = target.copy()
    if (value["resourse"], value["domain_code"]) in copy_target:
        copy_target.remove((value["resourse"], value["domain_code"]))
    DOMAIN_CONFIG[key]["target"] = copy_target
