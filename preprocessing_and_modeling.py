import pandas as pd
import re
from nltk import wordpunct_tokenize
from langdetect import detect
from tqdm.notebook import tqdm
import pymorphy2
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
tqdm.pandas()

# Загрузка данных
req = pd.read_csv('full_req_and_resp.csv')[['id', 'requirement']]

# Приведение к нижнему регистру
req['requirement'] = req.requirement.str.lower()

# Удаление всех символов кроме русских и латинских букв
req['requirement'] = req.requirement.apply(lambda x: re.sub(r'[^A-Za-zА-Яа-я\s]', '', x))

# Функция для определения языка текста
def detect_language(text):
    try:
        tokens = wordpunct_tokenize(text)
        words = [word.lower() for word in tokens]
        if any(word.isalpha() for word in words):
            return detect(" ".join(words))
        else:
            return None
    except Exception as e:
        return None

# Фильтрация текстов по наличию русских слов
russian_texts = req['requirement'].progress_apply(lambda text: detect_language(text) == 'ru')

# Оставляем только тексты на русском языке
req = req[russian_texts]

# Создаем экземпляр класса PyMorphy
morph = pymorphy2.MorphAnalyzer()

# Функция для лемматизации 
def lemmatize_text(text):
    words = text.split()
    lemmatized_words = [morph.parse(word)[0].normal_form for word in words]
    return ' '.join(lemmatized_words)

# Лемматизация столбца 'requirement' 
req['lemmatized_requirement'] = req['requirement'].progress_apply(lemmatize_text)

# Создание множества стоп-слов
stop_words = set(["и", "в", "во", "не", "что", "он", "на", "я", "с", "со", "как", "а", "то", "все", "она", "так", "его", "но", "да", "ты", "к", "у", "же", "вы", "за", "бы", "по", "только", "ее", "мне", "было",
"вот", "от", "меня", "еще", "нет", "о", "из", "ему", "теперь", "когда", "даже", "ну", "вдруг",
"ли", "если", "уже", "или", "ни", "быть", "был", "него", "до", "вас", "нибудь", "опять", "уж",
"вам", "сказал", "ведь", "там", "потом", "себя", "ничего", "ей", "может", "они", "тут", "где",
"есть", "надо", "ней", "для", "мы", "тебя", "их", "чем", "была", "сам", "чтоб", "без", "будто",
"чего", "раз", "тоже", "себе", "под", "будет", "ж", "тогда", "кто", "этот", "того", "потому",
"этого", "какой", "совсем", "ним", "здесь", "этом", "один", "почти", "мой", "тем", "чтобы",
"нее", "сейчас", "были", "куда", "зачем", "всех", "никогда", "можно", "при", "наконец", "два",
"об", "другой", "хоть", "после", "над", "больше", "тот", "через", "эти", "нас", "про", "всего",
"них", "какая", "много", "разве", "три", "эту", "моя", "впрочем", "хорошо", "свою", "этой",
"перед", "иногда", "лучше", "чуть", "том", "нельзя", "такой", "им", "более", "всегда", "конечно",
"всю", "между", 'работа', 'год', 'наличие', 'понимание', 'навык', 'уверенный', 'опыт', 'умение', 'знание', "менее", "иметь", "знать", "обладать", "владеть", "дмс", "москва", "офис", "день", "удаленный", "компания", "работать",  "приветствоваться", "уровень", "заработный", "желательно", "условие", "владение", "развитой", "способность",   "готовность", "нужный", "постоянно", "разъездной", "другой", "удалённый", "формат", "который", "наш", "команда", "мы",  "возможность", "это", "требование", "больший", "важный", "один", "степень", "понимать", "хотеть", "видеть", "плюс", "далее",  "пр", "требоваться", "два", "соответствовать", "плюс", "хороший", "отлично", "часть", "обладание", "реализовывать", "реализовать",  "предстоять", "уметь",  "middle", "так", "твой", "конец", "свой", "уметь", "такой", "ее", "тема", "каждый", "ление", "начальный", "авто", "тп", "мочь", "базовый", "основа", "абс", "необходимо", "данный", "ваш", "кандидат", "рассматривать", "готовый", "также", "студент", "неделя", "специалист", "рабочий", "пройти", "полный", "собеседование", "способный", "сам", "письмо", "сопроводительный", "должный", "возможный", "возможно", "вакансия", "резюме", "ссылка", "указать", "стать", "отклик", "обязательно",  "случай", "стать", "полгода", "пять", "общий", "быть", "ты", "нужно", "весь", "являться", "преимущество", "уверенно", "дело",  "устроить", "рф", "любой", "выполнить", "три", "выше", "место", "ниже",  "тип", "график", "внутри", "интересный", "тк",  "гибридный", "страхование", "дать", "рука", "первый", "следующий", "какой", "человек", "среднее", "ожидать", "хотя", "схожий", "знак", "друг", "желательный", "область", "знакомство", "создавать", "заниматься", "задание", "коммерческий", "задача", "продвинуть", "опытный", "автомобиль", "основной" "ждать", "период", "например", "обязанность", "разный", "знакомый", "несколько", "центр", "крупный", "фз", "россия", "достаточный", "особенно", "необходимость", "программа", "направление", "целое", "тд", "непосредственный", "месяц", "основной", "рамка", "требоваться", "существующий", "различный", "вид","тч",  "важно", "отличный", "зарплата", "подразделение", "внешний", "внутренний", "обязанность", "компетенция", "всё", "специальный", "популярный", "сфера", "роль", "использовать", "применять", "иить", "др", "сотрудник", "обычный", "самый", "ит", "корпоративный", "отдельный", "иной", "лида", "пользоваться", "отдел", "глубокий", "командировка", "ведущий", "производитель", "конкретный", "it", "включая",  "использование", "прочий", "ждать", "действие", "большой", "собственный",  "учта",  "квалификация", "повышение", "senior", "делать", "реальный", "минимум", "сторонний", "itкомпания", "число", "смежный", "рекомендация", "иметься", "дополнительный", "сделать", "основный", "объем", "минимальный", "вещь", "применение", "небольшой", "предпочтительно", "предпочтение", "возникать", "минус", "час", "позволить", "предпочтительный", "очень", "легко", "пройтись",
"карьерный", "либо", "вариант", "категория", "новый", "ключевой", "представление", "ти", "тихий", "релевантный", "необходимый", "международный", "подход", "связанный", "ориентироваться", "сектор", "крепкий", "страховой", "ключевой", "пример", "использоваться", "любить", "срок", "разработка", "руководитель", "характер", "коллега", "заявка", "участник", "оценить", "банк",
"время", "подобный", "вопрос", "остальной", "рассмотреть", "выпускник", "вуз", "выбрать", "консалтинг", "лицо", "юридически", "обязательный", "преимущественно",  "официальный", "получить", "собственный", "классический", "этап", "сторона", "сильные", "конструктивный", "слово", "главное", "смочь", "чёткий", "право", "делиться", "распространять", "государственный", "учреждение", "эксперт", "постоянный", "получение", "частность", "последний", "качество", "старший", "прохождение", "наём", "компонент", "прямой", "телефон", "специализированный", "окончить", "го", "подтвердить",  "написать", "профессионал", "профессионально", "участвовать", "выполнение", "etc", "менеджер", "одежда", "рассматриваться", "ждм", "трх",  "отсутствие", "профессия", "оформление", "успешно", "член", "вместе", "интернет", "телек", "мбитый", "гарнитур", "микрофон", "группа", "сообщение", "иса", "бояться",   "необходимый", "медицинский", "вебкамера", "стандартный", "желание", "предлагать", "разработчик", "услуга", "пк", "программист", "язык", "организовать", "получать", "ведение", "жизненный", "чётко",
"стремление", "junior", "позиция", "компьютер"
    ])

# Удаление стоп-слов
documents = [" ".join([word 
                       for word in document.split() 
                       if word not in stop_words and len(word) > 1]) 
             for document in req.lemmatized_requirement]

# Применение CountVectorizer
vectorizer = CountVectorizer(analyzer='word', min_df = 50)
X = vectorizer.fit_transform(documents)

# Применение LDA
num_topics = 48
lda = LatentDirichletAllocation(n_components=num_topics, random_state=777, max_iter=100)
lda.fit(X)

# Вывод тем и связанных с ними слов
feature_names = vectorizer.get_feature_names_out()

for topic_idx, topic_words in enumerate(lda.components_):
    top_words_idx = topic_words.argsort()[-10:][::-1]
    top_words = [feature_names[i] for i in top_words_idx]
    print(f"Тема {topic_idx + 1}: {', '.join(top_words)}")
