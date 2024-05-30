import requests
import json
import time
import pandas as pd

# Импорт предварительно выгруженных с hh.ru и обработанных списков регионов (areas), специальностей в информационных технологиях (inf_roles) и отраслей компаний (industries) вместе с их id
areas         = pd.read_csv('areas_ru.csv')
inf_roles     = pd.read_csv('inf_roles.csv')
industries    = pd.read_csv('industries.csv')

# Цикл для выгрузки вакансий
vacancies = pd.DataFrame()

for area_id, area in areas.iterrows():
    
    for industry_id, industry in industries.iterrows():
        
        for role_id, role in inf_roles.iterrows():
            
            def get_page(page=0):
                params = {
                    'page': page,
                    'area': area_id,
                    'per_page': 100,
                    'industry' : industry_id,
                    'professional_role': role_id
                }

                req = requests.get('https://api.hh.ru/vacancies', params)
                data = req.content.decode()
                req.close()
                return data

            j_obs = []

            for page in range(0, 20):
                j_ob = json.loads(get_page(page))
                if 'items' in j_ob:
                    j_obs.extend(j_ob.get('items', []))
                else:
                    print("Key 'items' not found in the response.")
                    break
                if 'pages' in j_ob and (j_ob['pages'] - page) == 0:
                    break
                time.sleep(1)
                
            df = pd.DataFrame(j_obs)
            
            if not df.empty:
                df = df[['id', 'name', 'salary', 'employer', 'snippet',
                            'schedule', 'experience', 'employment']]
                df['area'] = area[0]
                df['industry'] = industry[0]
                df['role'] = role[0]
                vacancies = pd.concat([vacancies, df])
            
            print('Done', area[0], industry[0], role[0])

vacancies.to_csv('all.csv')

# Так как в Москве, даже с учетом перебора параметров, глубина некоторых запросов для разработчиков была больше 2000, было решено использовать дополнительные переменные и сделать цикл с перебором максимального числа параметров, чтобы охватить все вакансии

experience = ["noExperience", "between1And3", "between3And6", "moreThan6"]
employment = ["full", "part", "project", "volunteer", "probation"]
schedule = ["fullDay", "shift", "flexible", "remote","flyInFlyOut"]

dev_msk = pd.DataFrame()
for i in experience:
    for j in employment:
        for k in schedule:
            def get_page(page=0):
                    params = {
                        'page': page,
                        'area': 1,
                        'per_page': 100,
                        'industry' : 7,
                        'professional_role': 96,
                        'experience': i,
                        'employment': j,
                        'schedule': k
                    }

                    req = requests.get('https://api.hh.ru/vacancies', params)
                    data = req.content.decode()
                    req.close()
                    return data

            j_obs = []

            for page in range(0, 20):
                j_ob = json.loads(get_page(page))
                if 'items' in j_ob:
                    j_obs.extend(j_ob.get('items', []))
                else:
                    print("Key 'items' not found in the response.")
                    break
                if 'pages' in j_ob and (j_ob['pages'] - page) == 0:
                    break
                time.sleep(1)

            df = pd.DataFrame(j_obs)

            if not df.empty:
                df = df[['id', 'name', 'salary', 'employer', 'snippet',
                            'schedule', 'experience', 'employment']]
                df['area'] = 'Москва'
                df['industry'] = 'Информационные технологии, системная интеграция, интернет'
                df['role'] = 'Программист, разработчик'
                dev_msk = pd.concat([dev_msk, df])

            print('Done', i, j, k)

# Объединение двух полученных датафреймов
vacancies = vacancies.query('not (area == "Москва" and industry == "Информационные технологии, системная интеграция, интернет" and role == "Программист, разработчик")')
vacancies_full = pd.concat([vacancies, dev_msk])
vacancies_full = vacancies_full.reset_index(drop=True)
vacancies_full.to_csv('vacancies_full.csv', index=False)

# Список id всех полученных на предыдущем шаге вакансий
ids = list(pd.read_csv('id.csv').id)

# Цикл для выгрузки полных текстов вакансий по id
a = 0
texts = pd.DataFrame()
for i in ids:
    req = requests.get('https://api.hh.ru/vacancies/' + str(i))
    data = json.loads(req.text)['description']
    df = pd.DataFrame({'id': [i], 'text': [data]})
    texts = pd.concat([texts, df])
    print(a)
    a += 1
    
    time.sleep(0.5)
    
texts.to_csv('all_texts.csv')
