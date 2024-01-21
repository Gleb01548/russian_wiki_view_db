PATH_TEMP_FILES = "/data/wiki_data/temp_files"
PATH_DOMAIN_DATA = "/data/wiki_data/domain_data"
PATH_SUCCESS = "/data/wiki_data/_success"
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
                "Среда",
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
                "Информация за {{ ds }} ({{ day_of_week }}). Период: {{ date_period_type }}"  # noqa
                "\n"
                "\nОбщее число просмотров википедии: {{ count_views }} {{ numerical_characteristic }} "  # noqa
                "(прирост: {{ views_increment_percent }}% )\n"  # noqa
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"  # noqa
            ),  # noqa
            "message_new_pages": (
                "{{ wikipedia_segment }}\n"
                "Разница с прошедшим периодом ({{ date_period_type }}).\n"
                "\n"
                "Новые страницы в топе:\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_go_out_pages": (
                "{{ wikipedia_segment }}\n"
                "Страницы которые вышли из топа:\n"
                "\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_difference_pages": (
                "{{ wikipedia_segment }}\n"
                "Изменения среди тех страниц, которые в топе остались:\n"
                "\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),
        },
    },  # русский
    "en": {
        "domain_code": "en",
        "time_correction": -4,
        "resourse": "eng_Latn",
        "translate_other_languages": True,
        "send_massages": True,
        "tags": ["#english", "#en"],
        "wikipedia_segment": "🇺🇸🇬🇧English🇬🇧🇺🇸",
        "message_settings": {
            "numerical_characteristic": {
                "day": "thds.",
                "week": "m.",
                "month": "m.",
                "year": "m.",
            },
            "date_period_type_translate": {
                "day": "day",
                "week": "week",
                "month": "month",
                "year": "year",
            },
            "day_of_week_translate": [
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ],
            "icon": "Индикатор | Indicator",
            "rank_now": "Curr. rating | ",
            "rank_last": "Past rating | ",
            "page_name": "Page name | ",
            "page_name_translate": "Name translation | ",
            "sum_views": "Number of views | ",
            "increment_percent": "Increase (in %) | ",
            "message_top_now": (
                "{{ wikipedia_segment }}\n"  # noqa
                "Information for {{ ds }} ({{ day_of_week }}). Period {{ date_period_type }}"  # noqa
                "\nTotal number of views on wikipedia: {{ count_views }} {{ numerical_characteristic }} "  # noqa
                "(increment: {{ views_increment_percent }}% )\n"  # noqa
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"  # noqa
            ),  # noqa
            "message_new_pages": (
                "{{ wikipedia_segment }}\n"
                "Difference from the previous period ({{ date_period_type }}).\n"
                "\n"
                "New pages at the top:\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_go_out_pages": (
                "{{ wikipedia_segment }}\n"
                "Pages that came out of the top:\n"
                "\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_difference_pages": (
                "{{ wikipedia_segment }}\n"
                "The changes among those pages in the top remain:\n"
                "\n"
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),
        },
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
    },  # персидский
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
}
