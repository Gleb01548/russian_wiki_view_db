load_url = "https://store.steampowered.com/charts/mostplayed"
path_save_json = "/data/steam_data/json"
path_save_script = "/data/steam_data"
path_save_analysis = "/data/steam_data/analysis"
path_save_messages = "/data/steam_data/messages"
columns_for_table_day = ["rank", "game_name", "players_count"]
columns_increment_for_table_day = [
    "increment_percent_players",
    "game_name",
    "players_count",
    "rank_actual",
    "rank_prior",
]
columns_for_table_not_day = ["rank", "game_name", "top_for_period", "avg_for_period"]
columns_increment_for_table_not_day = [
    "increment_days_in_top",
    "increment_percent_avg_players",
    "game_name",
    "top_for_period_actual",
]
columns_increment_for_table_csv_not_day = [
    "increment_days_in_top",
    "increment_percent_avg_players",
    "game_name",
    "top_for_period_actual",
    "top_for_period_prior",
    "avg_for_period_actual",
    "avg_for_period_prior",
    "rank_actual",
    "rank_prior",
]

MESSAGE_CONFIG = {
    "ru": {
        "bot_token": "STEAM_RU_BOT",
        "telegram_id": "STEAM_RU",
        "day_of_week_translate": [
            "Воскресенье",
            "Понедельник",
            "Вторник",
            "Среда",
            "Четверг",
            "Пятница",
            "Суббота",
        ],
        "date_period_type_translate": {
            "day": "день",
            "week": "неделя",
            "month": "месяц",
            "year": "год",
        },
        "columns_name": {
            "rank": "ранг",
            "game_name": "имя игры",
            "players_count": "макс. игроков, тыс.",
            "increment_percent_players": "рост игроков, %",
            "rank_actual": "акт. ранг",
            "rank_prior": "пред. ранг",
            "top_for_period": "дни в топе",
            "avg_for_period": "ср. макс. игроков, тыс.",
            "increment_days_in_top": "изм. дней в топе",
            "increment_percent_avg_players": "изм. ср. макс. игроков, %",
            "top_for_period_actual": "акт. дни в топе",
            "top_for_period_prior": "пред. дни в топе",
            "avg_for_period_actual": "акт. ср. макс. игроков",
            "avg_for_period_prior": "пред. ср. макс. игроков",
        },
        "message_top_now": (
            "Информация за #{{ ds }} ({{ day_of_week }}). Период: #{{ date_period_type }}.\n\n"
            "{{ col }}\n"
            "{{ pages_data }}"
        ),
        "message_new_games": (
            "Разница с прошедшим периодом ({{ date_period_type }}).\n\n"
            "Новые страницы в топе:\n"
            "{{ col }}\n"
            "{{ pages_data }}"
        ),
        "message_go_out_games": (
            "Игры которые вышли из топа:\n" "{{ col }}\n" "{{ pages_data }}"
        ),
        "message_difference_games": (
            "Изменения среди тех игр, которые в топе остались:\n"
            "{{ col }}\n"
            "{{ pages_data }}\n\n"
            "Более подробно об изменениях среди тех игр, которые в топе остались в файле excel."
        ),
    },
    "en": {
        "bot_token": "STEAM_EN_BOT",
        "telegram_id": "STEAM_EN",
        "day_of_week_translate": [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ],
        "date_period_type_translate": {
            "day": "day",
            "week": "week",
            "month": "month",
            "year": "year",
        },
        "columns_name": {
            "rank": "rank",
            "game_name": "game name",
            "players_count": "max. players, th.",
            "increment_percent_players": "player grth, %",
            "rank_actual": "cur. rank",
            "rank_prior": "pri. rank",
            "top_for_period": "top days",
            "avg_for_period": "av. max. players, th.",
            "increment_days_in_top": "days grth top",
            "increment_percent_avg_players": "av. max. players, th. изм. grth, %",
            "top_for_period_actual": "cur. top days",
            "top_for_period_prior": "pri. top days",
            "avg_for_period_actual": "cur. av. max. players",
            "avg_for_period_prior": "pri. av. max. players",
        },
        "message_top_now": (
            "Information for #{{ ds }} ({{ day_of_week }}). Period #{{ date_period_type }}.\n\n"
            "{{ col }}\n"
            "{{ pages_data }}"
        ),
        "message_new_games": (
            "Difference from the previous period ({{ date_period_type }}).\n\n"
            "New games at the top:\n"
            "{{ col }}\n"
            "{{ pages_data }}"
        ),
        "message_go_out_games": (
            "Games that are out of the top:\n" "{{ col }}\n" "{{ pages_data }}"
        ),
        "message_difference_games": (
            "Changes among those games that remained in the top:\n"
            "{{ col }}\n"
            "{{ pages_data }}\n\n"
            "More details on the changes among those games in the top remaining in the excel file."
        ),
    },
}
