import pandas as pd
from bs4 import BeautifulSoup
import re

# Загрузка полных текстов вакансий
texts = pd.read_csv('all_texts.csv')

# Заменяем открывающие теги <p>, означающие новый параграф на символ '***', чтобы во время дальнейшей чистки тегов они не удалились
texts['text'] = texts['text'].str.replace('<p>', '***')

# Определяем функцию для удаления всех тегов 
def clean_html(text):
    soup = BeautifulSoup(text, 'html.parser')
    text_without_tags = soup.get_text(separator=' ', strip=True)
    return text_without_tags

# Удаляем теги 
texts['text'] = texts['text'].apply(clean_html)

# Делим текст по параграфам и знаку «:», так как обычно после него идет перечисление требований или обязанностей
texts['text'] = texts['text'].apply(lambda x: [i.split(':') for i in x.split('***')])

# Читаем файл с краткой информацией по вакансиям, которые содержит неполные требования и обязанности, а также список id
req_and_resp = pd.read_csv('vacancies.csv')
ids = list(pd.read_csv('id.csv').id)

# Цикл для выделения требований и обязанностей из полных текстов вакансий на основе их кратких версий
full_req_and_resp = pd.DataFrame()
for i in ids:
    requirement = req_and_resp[req_and_resp.id == i].requirement.to_list()[0].strip()
    requirement = re.sub(r'[^\w\s]', '', requirement)

    responsibility = req_and_resp[req_and_resp.id == i].responsibility.to_list()[0].strip()
    responsibility = re.sub(r'[^\w\s]', '', responsibility)

    texts = full_text[full_text.id == i].text.to_list()[0]

    full_req = ''
    full_resp = ''

    for lst in texts:
        for text in lst:
            text_cleaned = re.sub(r'[^\w\s]', '', text)
            if requirement[-10:] in text_cleaned:
                full_req = text
            if responsibility[-10:] in text_cleaned:
                full_resp = text

    df = pd.DataFrame({'id': [i],
                       'requirement': [full_req],
                       'responsibility': [full_resp]})

    full_req_and_resp = pd.concat([full_req_and_resp, df])

# Сохраняем результат
full_req_and_resp.to_csv('full_req_and_resp.csv')
