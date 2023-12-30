# wiki_view_db

Проект скачивает данные о просмотрах википедии за каждый час.
Заливает их в базу данных. Каждые сутки проводится подсчет топ 100 статей на википедии по просмотрам, какие статьи по сравнению с прошлым периодом оказались в топе, какие вышли, как менялась популярность статей, которые в топе остались, также предоставляется график количества просмотров.

Считаются периоды: день, неделя, месяц, год. 

Просматриваются следующие сегменты википедии: русский, английский, испанский, португальский, французский, немецкий, польский, украинский, турецкий, персидский, арабский, арабский египетский, хинди, индонезийский, иврит.
По каждому сегменту википедии статистика и анализ ведется отдельно, также имеются переводы на русский и английский всех названий статей. Перевод с помощью модели машинного обучения, которая развернута у меня на GPU (перевод не очень, но бесплатный).

Полученные данных заливаются в тг каналы: 
Статистика по русскому сегменту: https://t.me/+AKyq4pvKH1BhMTA6
Переводы на русский с других сегментов: https://t.me/+uHrhEUTyUsowYjQ6
Так как очень много переводов и сообщений, сделал тг каналы куда отправляется переводы по группам стран:
- Западная Европа и Америка (английская, испанская, португальская, французская, немецкая): https://t.me/+lVV5YNe2R_8yYWRi
- Восточная Европа и СНГ (русский, польский, украинский): https://t.me/+lVV5YNe2R_8yYWRi
- Ближний Восток (турецкий, персидский, арабский, арабский египетский, иврит): https://t.me/+yEb-JsFAJHhlOTli
- Азия (индийская, индонезийская): https://t.me/+42otQvKhqaRkNGFi

Также сделал тоже самое, но только все на английском: 
- статистика только по английскому сегменту https://t.me/+us6JcXEf3_kzZGY6
- переводы на английский: https://t.me/+Yh9A4d24gSwxZTAy
- Западная Европа и Америка: https://t.me/+qYLqkH70zIM0Mjgy
- Восточная Европа и СНГ: https://t.me/+L_L1kSpouy5hOGIy
- Ближний Восток: https://t.me/+3HE0wRMbg0IyZGI6
- Азия: https://t.me/+QrAv_uAk_2phYjYy

Стек: Postgres Clikchouse Airflow flask pytorch 
