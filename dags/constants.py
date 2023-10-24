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
                "Информация за {{ ds }} период: {{ date_period_type }} ({{ day_of_week }})"  # noqa
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
                "Information for {{ date_period_type }} {{ ds }} ({{ day_of_week }})"  # noqa
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
                '\n'
                "{{ col1 }}{{ col2 }}{{ col3 }}{{ col4 }}{{ col5 }}{{ col6 }}{{ col7 }}"  # noqa
                "{{ pages_data }}"
            ),  # noqa
            "message_difference_pages": (
                "{{ wikipedia_segment }}\n"
                "The changes among those pages in the top remain:\n"
                '\n'
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
    # "it": {
    #     "domain_code": "it",
    #     "time_correction": 2,
    #     "resourse": "ita_Latn",
    #     "translate_other_languages": False,
    #     "send_massages": False,
    #     "tags": ["#italian", "#it"],
    #     "wikipedia_segment": "🇮🇹Italian🇮🇹",
    # },  # итальянский
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
    # "ja": {
    #     "domain_code": "ja",
    #     "time_correction": 9,
    #     "resourse": "jpn_Jpan",
    #     "translate_other_languages": False,
    #     "send_massages": False,
    #     "tags": ["#japanese", "#ja"],
    #     "wikipedia_segment": "🇯🇵Japanese🇯🇵",
    # },  # японский
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
    }  # иврит
    # "hy": {
    #     "domain_code": "hy",
    #     "time_correction": 4,
    #     "resourse": "hye_Armn",
    #     "translate_other_languages": False,
    #     "send_massages": False,
    #     "tags": ["#armenian", "#hy"],
    #     "wikipedia_segment": "🇦🇲Armenian🇦🇲",
    # },  # армянский
    # "az": {
    #     "domain_code": "az",
    #     "time_correction": 4,
    #     "resourse": "azj_Latn",
    #     "translate_other_languages": False,
    #     "send_massages": False,
    #     "tags": ["#azerbanian", "#az"],
    #     "wikipedia_segment": "🇦🇿Azerbanian🇦🇿",
    # },  # азербанжанский
    #     "sr": {
    #         "domain_code": "sr",
    #         "time_correction": 2,
    #         "resourse": "srp_Cyrl",
    #         "translate_other_languages": False,
    #         "send_massages": False,
    #         "tags": ["#serbian", "#sr"],
    #         "wikipedia_segment": "🇷🇸Serbian🇷🇸",
    #     },  # сербский
}

GROUP = {
    "western_europe_america": ["en", "es", "pt", "fr", "de"],
    "eastern_europe_cis": [
        "ru",
        "uk",
        "pl",
    ],
    "middle_east": ["tr", "fa", "arz", "ar", "he"],
    "asia": ["hi", "id"],
}

for key_domen in DOMAIN_CONFIG.keys():
    if DOMAIN_CONFIG[key_domen]["send_massages"]:
        new_dict = {key: [] for key in DOMAIN_CONFIG.keys()}
        for key, values in GROUP.items():
            for domain_code in values:
                if domain_code == key_domen:
                    new_dict[domain_code].append(f"{key_domen.upper()}_GROUP")
                else:
                    new_dict[domain_code].append(f"{key_domen.upper()}_{key.upper()}")
                    new_dict[domain_code].append(f"{key_domen.upper()}_TRANSLATION")
        # print(new_dict)
        DOMAIN_CONFIG[key_domen]["message_settings"]["group_tokens"] = new_dict


SEND_MESSAGES = [
    key for key in DOMAIN_CONFIG.keys() if DOMAIN_CONFIG[key]["send_massages"]
]

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

BOTS = {
    "ru": ["TELEGRAM_TOKEN_BOT", "TELEGRAM_TOKEN_BOT_1", "TELEGRAM_TOKEN_BOT_2"],
    "en": [
        "EN_TELEGRAM_TOKEN_BOT",
        "EN_TELEGRAM_TOKEN_BOT_1",
        "EN_TELEGRAM_TOKEN_BOT_2",
    ],
}


# text = "start_oper >> "
# for index_2 in range(len(DOMAIN_CONFIG)):
#     text += f"groups_message[{index_2}] >> "
# text = text.removesuffix(" >> ")

# print(text)

# text = "start_oper >> "
# for index_2 in range(len(DOMAIN_CONFIG), len(DOMAIN_CONFIG) * 2):
#     text += f"groups_message[{index_2}] >> "
# text_2 = text.removesuffix(" >> ")

# print(text_2)
