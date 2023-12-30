import os
import json
import requests
import pendulum
from flask import Flask, request
import threading

app = Flask(__name__)

lock = threading.Lock()

dict_cahce = {}

path_files = "data/cache"
files_list = [
    file_name for file_name in os.listdir(path_files) if file_name.endswith(".json")
]

for cahce_name in files_list:
    with open(os.path.join(path_files, cahce_name), "r") as file:
        cahce_name = cahce_name.removesuffix(".json")
        dict_cahce[cahce_name] = {}
        for line in file:
            line = json.loads(line)
            dict_cahce[cahce_name][line["source"]] = line["target"]


def _create_files(path_file: str):
    with open(path_file, "w"):
        pass


def add_data_to_cahce(
    path_file: str, dict_cache: dict, source_sents: str, target_sents: str
):
    date = pendulum.now("UTC").to_date_string()
    for source, target in zip(source_sents, target_sents):
        with open(path_file, "a") as file:
            dict_cache[source] = target
            json.dump(
                {"source": source, "target": target, "date": date},
                file,
                ensure_ascii=False,
            )
            file.write("\n")


@app.route("/translate", methods=["POST"])
def translate():
    IAM_TOKEN = os.environ.get("TRANSLATE_TOKEN_API")
    source = request.form.get("source")
    target = request.form.get("target")
    sentences = request.form.getlist("sentences")

    cahce_name = f"{source}_{target}"
    cahce_name_reverse = f"{target}_{source}"
    path_cahce_name = os.path.join(path_files, f"{cahce_name}.json")
    path_cahce_name_reverse = os.path.join(path_files, f"{cahce_name_reverse}.json")
    with lock:
        if not os.path.exists(path_cahce_name):
            _create_files(path_cahce_name)
            _create_files(path_cahce_name_reverse)
            dict_cahce[cahce_name] = {}
            dict_cahce[cahce_name_reverse] = {}

        cahce_now = dict_cahce[cahce_name]
        cahce_now_reverse = dict_cahce[cahce_name_reverse]
        data_to_translate = []
        for sents in set(sentences):
            if sents not in cahce_now:
                data_to_translate.append(sents)
        print("слов для перевода", len(data_to_translate))
        if data_to_translate:
            body = {
                "sourceLanguageCode": source,
                "targetLanguageCode": target,
                "texts": data_to_translate,
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": "Api-Key {0}".format(IAM_TOKEN),
            }

            response = requests.post(
                "https://translate.api.cloud.yandex.net/translate/v2/translate",
                json=body,
                headers=headers,
            ).json()["translations"]
            target = [i["text"] for i in response]

            add_data_to_cahce(
                path_file=path_cahce_name,
                dict_cache=cahce_now,
                source_sents=data_to_translate,
                target_sents=target,
            )

            add_data_to_cahce(
                path_file=path_cahce_name_reverse,
                dict_cache=cahce_now_reverse,
                source_sents=target,
                target_sents=data_to_translate,
            )

        result = []
        for source_sent in sentences:
            result.append(cahce_now[source_sent])
        return result


if __name__ == "__main__":
    print("Стартуем!")
    app.run(host="0.0.0.0", port=8240)
