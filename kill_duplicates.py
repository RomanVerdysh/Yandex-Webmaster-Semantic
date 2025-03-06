import pandas as pd
import os
import re
from natasha import Segmenter, MorphVocab, Doc, NewsEmbedding, NewsMorphTagger
from collections import Counter
import csv

# Настройка пути
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Инициализация инструментов Natasha
segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)

# Функция для лемматизации и удаления стоп-слов
def lemmatize(text):
    text = re.sub(r'[^\w\s]', ' ', text)    # Заменить спецсимволы на пробелы
    text = re.sub(r'[-,]', ' ', text)  # Заменяет тире и запятые на пробелы
    text = re.sub(r'\s+', ' ', text).strip()    # Заменить множественные пробелы на одиночные и удалить ведущие и конечные пробелы

    doc = Doc(text)
    doc.segment(segmenter)
    doc.tag_morph(morph_tagger)
    lemmas = []
    for token in doc.tokens:
        token.lemmatize(morph_vocab)
        if token.pos not in ('ADP', 'PART', 'CONJ', 'PRCL'):  # Исключаем предлоги, частицы и союзы
            lemmas.append(token.lemma)
    return " ".join(sorted(lemmas))  # Возвращаем лемматизированный текст с отсортированными словами

# Загрузка данных
df = pd.read_csv('query_analytics.csv', delimiter=';')

# Преобразование числовых столбцов
numeric_columns = ['Показы', 'Клики', 'Ср. Позиция', 'Ср. CTR', 'Спрос', 
                   '% от Спроса', 'Прогноз кликов TOP-1', 'Прогноз кликов TOP-3', 'Прогноз кликов TOP-5']

df[numeric_columns] = df[numeric_columns].replace({',': '.'}, regex=True).astype(float)

# Обработка дублей
results = pd.DataFrame(columns=df.columns)
lemmatized_queries = df['Запрос'].apply(lemmatize)  # Здесь функция lemmatize - это ваша функция лемматизации

for query in lemmatized_queries.unique():
    similar_queries = df[lemmatized_queries == query]
    
    # Выбор основного запроса на основе максимального значения спроса
    main_query = similar_queries.loc[similar_queries['Спрос'].idxmax(), 'Запрос']
    
    # Подсчет сумм и средних значений
    result_row = {
        'URL': similar_queries['URL'].iloc[0],
        'Запрос': main_query,
        'Показы': similar_queries['Показы'].sum(),
        'Клики': similar_queries['Клики'].sum(),
        'Ср. Позиция': similar_queries['Ср. Позиция'].mean(),
        'Ср. CTR': similar_queries['Клики'].sum() / similar_queries['Показы'].sum() if similar_queries['Показы'].sum() > 0 else 0,
        'Спрос': similar_queries['Спрос'].max(),
        '% от Спроса': similar_queries['% от Спроса'].mean(),
        'Прогноз кликов TOP-1': similar_queries['Прогноз кликов TOP-1'].sum(),
        'Прогноз кликов TOP-3': similar_queries['Прогноз кликов TOP-3'].sum(),
        'Прогноз кликов TOP-5': similar_queries['Прогноз кликов TOP-5'].sum(),
        'Брендовый': similar_queries['Брендовый'].iloc[0],
        'Стоп-слова': similar_queries['Стоп-слова'].iloc[0],
    }
    
    results = pd.concat([results, pd.DataFrame([result_row])], ignore_index=True)

# Сохранить результаты в новый файл без кавычек
results.to_csv('query_analytics_lemmatization.csv', index=False, sep=';', encoding='utf-8-sig', quoting=csv.QUOTE_NONE, escapechar='\\')

# Заменить точки на запятые только в числовых строках, сохраняя URL
with open('query_analytics_lemmatization.csv', 'r', encoding='utf-8-sig') as file:
    lines = file.readlines()

header = lines[0]
processed_lines = [header]

for line in lines[1:]:
    parts = line.split(';')
    url = parts[0]  # Сохраняем URL как есть
    
    # Обрабатываем остальные части
    for i in range(1, len(parts)):
        if i in [2, 3, 4, 5, 6, 7, 8, 9, 10]:  # Индексы числовых столбцов
            # Если это число с точкой, форматируем его
            if '.' in parts[i]:
                try:
                    num = float(parts[i])
                    parts[i] = f"{num:.2f}".replace('.', ',')
                except:
                    pass
            parts[i] = parts[i].replace("'", "").replace('"', '')
    
    new_line = ';'.join(parts)
    processed_lines.append(new_line)

with open('query_analytics_lemmatization.csv', 'w', encoding='utf-8-sig') as file:
    file.writelines(processed_lines)
