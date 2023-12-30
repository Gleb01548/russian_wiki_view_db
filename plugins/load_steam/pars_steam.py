import os
import time
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from airflow.models import BaseOperator


class ParsSteam(BaseOperator):
    """
    Класс загружает названия и количество игроков топ 100
    самых популярных игра за последние 24 часа.
    Данные сохраняет как json и готовит скрипт для sql
    """

    template_fields = ("ds",)

    def __init__(
        self,
        url: str,
        path_save_script: str,
        path_save_json: str,
        ds="{{ ds }}",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.url = url
        self.ds = ds
        self.path_save_script = path_save_script
        self.path_save_json = path_save_json

    def create_path(self):
        self.path_save_script = os.path.join(
            self.path_save_script, f"script_{self.ds}.sql"
        )
        self.path_save_json = os.path.join(
            self.path_save_json, f"data_steam_{self.ds}.json"
        )

    def make_list(self, driver):
        games = driver.find_element(
            By.CLASS_NAME, "weeklytopsellers_ChartPlaceholder_3sJkw"
        )

        list_games = []
        for i in games.find_elements(By.TAG_NAME, "tr")[1:]:
            name = i.find_element(By.CLASS_NAME, "weeklytopsellers_GameName_1n_4-").text
            peak = i.find_element(
                By.CLASS_NAME, "weeklytopsellers_PeakInGameCell_yJB7D"
            ).text
            list_games.append(
                (
                    name.removesuffix("\n(not available in your region)"),
                    int(peak.replace(",", "")),
                )
            )
        print(list_games)
        return list_games

    def switch(self, driver):
        print("нажимаем на кнопку для переключения")
        driver.find_element(By.CLASS_NAME, "DialogDropDown_CurrentDisplay").click()
        print("ищем элемент для переключения")
        list_elements = driver.find_elements(By.TAG_NAME, "div")
        for i in list_elements:
            if (
                i.get_attribute("class")
                == "DialogMenuPosition visible contextmenu_ContextMenuFocusContainer_2qyBZ"
            ):
                need_elements = i
        new_list = need_elements.find_elements(By.TAG_NAME, "div")

        for i in new_list:
            print(i.get_attribute("class"))
            print(i.text)
            print()
            if (
                i.get_attribute("class") == "dropdown_DialogDropDownMenu_Item_1R-DV"
                and i.text == "By Daily Players"
            ):
                need_element = i
        need_element.click()
        time.sleep(5)

    def save_json(self, data: list):
        with open(self.path_save_json, "w") as f:
            json.dump({"data": data}, f)

    def make_sql_script(self, data: list):
        with open(self.path_save_script, "w") as f:
            f.write(f"insert into steam.steam_data (name, players_count) values")
            max_index = len(data) - 1
            for index, (name, players_count) in enumerate(data):
                symbol = ",\n" if max_index > index else ";"
                name = name.replace("'", "''")
                f.write(f"('{name[:2000]}', '{players_count}'){symbol}")

    def execute(self, context) -> None:
        self.create_path()
        print("Options")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        print("драйвер")
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), options=options
        )
        print("подключен")
        print("полечение данных с страницы")
        driver.get("https://store.steampowered.com/charts/mostplayed")
        print("получено")
        time.sleep(5)
        print(driver.title)
        self.switch(driver)
        games_24_hours = self.make_list(driver)
        self.save_json(games_24_hours)
        self.make_sql_script(games_24_hours)
