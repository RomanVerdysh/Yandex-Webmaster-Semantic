import requests
import csv
from datetime import datetime, timedelta
from urllib.parse import urlparse
from collections import defaultdict
import os
import re

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

def read_access_token(file_path):
    try:
        with open(file_path, 'r') as file:
            token = file.read().strip()
        return token
    except Exception as e:
        print(f"Ошибка при чтении файла с токеном: {e}")
        return None
    
ACCESS_TOKEN_FILE = 'access_token.txt'
ACCESS_TOKEN = read_access_token(ACCESS_TOKEN_FILE)    

USER_API_URL = 'https://api.webmaster.yandex.net/v4/user/'
HOSTS_API_URL = 'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/'
QUERIES_API_URL = 'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/query-analytics/list'
SEARCH_QUERIES_HISTORY_API_URL = 'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/search-queries/all/history'
URLS_FILE = 'urls.txt'
OUTPUT_CSV = 'query_analytics.csv'


def load_brand_names(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            brands = [line.strip().lower() for line in file]
        return brands
    except Exception as e:
        print(f"Ошибка при чтении файла с брендами: {e}")
        return []

def load_stop_words(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            stop_words = [line.strip().lower() for line in file]
        return stop_words
    except Exception as e:
        print(f"Ошибка при чтении файла со стоп-словами: {e}")
        return []

def get_user_id():
    headers = {
        'Authorization': f'OAuth {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    response = requests.get(USER_API_URL, headers=headers)
    if response.status_code == 200:
        user_info = response.json()
        print (user_info.get('user_id'))
        return user_info.get('user_id')
    else:
        print(f"Ошибка при получении user_id: {response.status_code} - {response.text}")
        return None

def get_hosts_list(user_id):
    headers = {
        'Authorization': f'OAuth {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    full_url = HOSTS_API_URL.format(user_id=user_id)
    response = requests.get(full_url, headers=headers)
    if response.status_code == 200:
        return response.json().get('hosts', [])
    else:
        print(f"Ошибка при получении списка хостов: {response.status_code} - {response.text}")
        return None

def get_host_id_for_url(hosts, url):
    clean_url = url.replace('https://', '').replace('http://', '').strip('/')
    for host in hosts:
        if clean_url in host['ascii_host_url']:
            return host['host_id']
    return None


def get_popular_queries_ctr(user_id, host_id):
    headers = {
        'Authorization': f'OAuth {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    url = f'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/search-queries/popular'
    params = {
        'query_indicator': ['AVG_CLICK_POSITION', 'TOTAL_SHOWS', 'TOTAL_CLICKS'],
        'order_by': 'TOTAL_CLICKS'  # Сортировка по количеству кликов
    }
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        # Получаем список популярных запросов
        items = response.json().get('queries', [])
        
        # Инициализируем словарь для хранения CTR по позициям
        ctr_by_position = {i: {'clicks': 0, 'impressions': 0} for i in range(1, 16)}
        
        # Обрабатываем каждый популярный запрос
        for item in items:
            # Извлекаем необходимые данные из словаря 'indicators'
            indicators = item.get('indicators', {})
            avg_click_position = indicators.get('AVG_CLICK_POSITION', 0)
            total_shows = indicators.get('TOTAL_SHOWS', 0)
            total_clicks = indicators.get('TOTAL_CLICKS', 0)
            
            # Если avg_click_position имеет значение None, заменяем на 0
            avg_click_position = avg_click_position or 0
            
            # Округляем позицию до ближайшего целого
            position = round(avg_click_position)
            
            # Если позиция находится в диапазоне от 1 до 15, обновляем соответствующую статистику
            if 1 <= position <= 15:
                ctr_by_position[position]['clicks'] += total_clicks
                ctr_by_position[position]['impressions'] += total_shows
        
        return items
    else:
        print(f"Ошибка при получении популярных запросов: {response.status_code} - {response.text}")
        return None

def save_ctr_to_csv(ctr_by_position, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Position', 'Average CTR']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for position, data in ctr_by_position.items():
            average_ctr = (data['clicks'] / data['impressions']) if data['impressions'] > 0 else 0
            writer.writerow({'Position': position, 'Average CTR': f"{average_ctr:.2f}".replace('.', ',')})
    print(f"Средний CTR для каждой позиции (1-15) сохранен в {output_file}")


def process_site_ctr(url, hosts, user_id, output_file):
    host_id = get_host_id_for_url(hosts, urlparse(url).netloc)
    # print(f"Получаем CTR для домена: {host_id}")
    if not host_id:
        print(f"Не удалось найти host_id для домена: {host_id}")
        return None

    # Получаем данные о популярных запросах
    popular_queries = get_popular_queries_ctr(user_id, host_id)
    
    # Формируем данные CTR по позициям
    if popular_queries:
        # Инициализируем словарь для хранения CTR по позициям
        ctr_by_position = {i: {'clicks': 0, 'impressions': 0} for i in range(1, 16)}

        for item in popular_queries:
            indicators = item.get('indicators', {})
            avg_click_position = indicators.get('AVG_CLICK_POSITION', 0) or 0
            total_shows = indicators.get('TOTAL_SHOWS', 0)
            total_clicks = indicators.get('TOTAL_CLICKS', 0)
            position = round(avg_click_position)
            if 1 <= position <= 15:
                ctr_by_position[position]['clicks'] += total_clicks
                ctr_by_position[position]['impressions'] += total_shows

        # Сохраняем средний CTR по позициям в CSV-файл
        save_ctr_to_csv(ctr_by_position, output_file)
    else:
        print(f"Не удалось получить популярные запросы для домена: {host_id}")

    return host_id



def get_query_analytics(user_id, host_id, target_url):
    headers = {
        'Authorization': f'OAuth {ACCESS_TOKEN}',
        'Content-Type': 'application/json'
    }
    full_url = QUERIES_API_URL.format(user_id=user_id, host_id=host_id)
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'fields': 'query,clicks,impressions,position',
        'limit': 500,
        'offset': 0,
        'filters': {
            'text_filters': [{
                'text_indicator': 'URL',
                'operation': 'TEXT_MATCH',
                'value': target_url
            }]
        }
    }
    # print(f"Запрос аналитики с {start_date} по {end_date} для URL: {target_url}")
    all_data = []
    while True:
        response = requests.post(full_url, headers=headers, json=params)
        if response.status_code == 200:
            data = response.json()
            queries = data.get('text_indicator_to_statistics', [])
            all_data.extend(queries)
            if len(queries) < params['limit']:
                break
            params['offset'] += params['limit']
        else:
            print(f"Ошибка при получении запросов поиска: {response.status_code} - {response.text}")
            return None
    return {'text_indicator_to_statistics': all_data}

def format_query_analytics(query_analytics):
    formatted_data = []
    query_summary = {}
    for item in query_analytics.get('text_indicator_to_statistics', []):
        query_text = item.get('text_indicator', {}).get('value', 'Неизвестный запрос')
        stats = item.get('statistics', [])
        if query_text not in query_summary:
            query_summary[query_text] = {
                'total_impressions': 0,
                'total_clicks': 0,
                'position_sum': 0,
                'ctr_sum': 0,
                'total_demand': 0,
                'percent_impressions_demand': 0,
                'count': 0
            }
        summary = query_summary[query_text]
        for stat in stats:
            field = stat.get('field')
            value = stat.get('value')
            if field == 'IMPRESSIONS':
                summary['total_impressions'] += value
            elif field == 'CLICKS':
                summary['total_clicks'] += value
            elif field == 'POSITION':
                summary['count'] += 1    
                summary['position_sum'] += value
            elif field == 'CTR':
                summary['ctr_sum'] += value
            elif field == 'DEMAND':
                summary['total_demand'] += value
        
    for query_text, summary in query_summary.items():
        average_position = round((summary['position_sum']) / summary['count']) if summary['count'] else 0
        average_ctr = round(summary['ctr_sum'] / (summary['count'] * 100),3) if summary['count'] else 0
        formatted_data.append({
            'query': query_text,
            'total_impressions': summary['total_impressions'],
            'total_clicks': summary['total_clicks'],
            'average_position': average_position,
            'average_ctr': average_ctr,
            'total_demand': summary['total_demand'],
            'percent_impressions_demand': summary['total_impressions']/summary['total_demand'],
        })
    return formatted_data

def calculate_average_ctr_per_position(query_analytics):
    position_data = defaultdict(lambda: {'clicks': 0, 'impressions': 0})
    for item in query_analytics.get('text_indicator_to_statistics', []):
        stats = item.get('statistics', [])
        for stat in stats:
            if stat.get('field') == 'POSITION' and stat.get('value') <= 15:
                pos = int(stat.get('value'))
                clicks = next((s['value'] for s in stats if s['field'] == 'CLICKS'), 0)
                impressions = next((s['value'] for s in stats if s['field'] == 'IMPRESSIONS'), 0)
                position_data[pos]['clicks'] += clicks
                position_data[pos]['impressions'] += impressions
    return position_data

def read_ctr_from_csv(file_path):
    ctr_data = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                position = int(row['Position'])
                ctr = float(row['Average CTR'].replace(',', '.'))
                ctr_data[position] = ctr
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")
    return ctr_data

def forecast_clicks(average_position, average_ctr_per_position, total_demand, total_clicks, ctr_file):
    # Читаем данные CTR из файла
    ctr_data = read_ctr_from_csv(ctr_file)
    
    # Получаем среднее значение CTR для 1, 1-3 и 1-5 позиций
    ctr_1_1 = [ctr_data.get(i, 0) for i in range(1, 2)]
    ctr_1_3 = [ctr_data.get(i, 0) for i in range(1, 4)]
    ctr_1_5 = [ctr_data.get(i, 0) for i in range(1, 6)]

    ctr_1 = average_ctr_per_position.get(1, {'clicks': 0, 'impressions': 0})
    ctr_3 = average_ctr_per_position.get(3, {'clicks': 0, 'impressions': 0})
    ctr_5 = average_ctr_per_position.get(5, {'clicks': 0, 'impressions': 0})

    # Среднее значение CTR
    ctr_1_av = sum(ctr_1_1) / len(ctr_1_1)
    ctr_3_av = sum(ctr_1_3) / len(ctr_1_3)
    ctr_5_av = sum(ctr_1_5) / len(ctr_1_5)

    ctr_1_value = (ctr_1['clicks'] / ctr_1['impressions']) if ctr_1['clicks'] > 0 else ctr_1_av
    ctr_3_value = (ctr_3['clicks'] / ctr_3['impressions']) if ctr_3['clicks'] > 0 else ctr_3_av
    ctr_5_value = (ctr_5['clicks'] / ctr_5['impressions']) if ctr_5['clicks'] > 0 else ctr_5_av

    forecast_1 = 0
    forecast_3 = 0
    forecast_5 = 0
    if average_position > 1:
        forecast_1 = round(ctr_1_value * total_demand)
    if average_position > 3:
        forecast_3 = round(ctr_3_value * total_demand)
    else:
        forecast_1 = total_clicks
        forecast_3 = total_clicks
        forecast_5 = total_clicks
    if average_position > 5:
        forecast_5 = round(ctr_5_value * total_demand)
    
    return forecast_1, forecast_3, forecast_5

def read_urls_from_file(file_path):
    with open(file_path, 'r') as file:
        urls = file.readlines()
    return [url.strip() for url in urls if url.strip()]

def save_results_to_csv(results, output_file, brands, stop_words):
    with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['URL', 'Запрос', 'Показы', 'Клики', 'Ср. Позиция', 'Ср. CTR', 'Спрос', '% от Спроса', 
                      'Прогноз кликов TOP-1', 'Прогноз кликов TOP-3', 'Прогноз кликов TOP-5', 'Брендовый', 'Стоп-слова']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        if csvfile.tell() == 0:
            writer.writeheader()

        for result in results:
            query_text = result['Запрос'].lower()

            # Проверка на брендовые запросы
            is_vital = 'Нет'
            for brand in brands:
                if brand in query_text:
                    is_vital = 'Да'
                    break

            # Проверка на стоп-слова
            stop_word_found = ''
            for stop_word in stop_words:
                if re.search(rf'\b{re.escape(stop_word)}\b', query_text):
                    stop_word_found = stop_word
                    break

            result['Брендовый'] = is_vital
            result['Стоп-слова'] = stop_word_found
            writer.writerow(result)
    # print(f"Результаты сохранены в {output_file}")

def save_ctr_to_csv(average_ctr_per_position, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Position', 'Average CTR']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for position, data in average_ctr_per_position.items():
            average_ctr = (data['clicks'] / data['impressions'] ) if data['impressions'] > 0 else 0
            writer.writerow({'Position': position, 'Average CTR': f"{average_ctr:.2f}".replace('.', ',')})
    # print(f"Средний CTR для каждой позиции сохранен в {output_file}")

def convert_csv_encoding(input_file, output_file, from_encoding='utf-8', to_encoding='cp1251'):
    with open(input_file, 'r', encoding=from_encoding) as infile, open(output_file, 'w', encoding=to_encoding, newline='') as outfile:
        reader = csv.reader(infile, delimiter=';')
        writer = csv.writer(outfile, delimiter=';')
        for row in reader:
            # Заменяем неподдерживаемые символы
            row = [s.encode(to_encoding, errors='replace').decode(to_encoding) for s in row]
            writer.writerow(row)
    print(f"Кодировка файла {output_file} изменена с {from_encoding} на {to_encoding}")


def process_url(url, hosts, user_id, output_file, brands, stop_words):
    print(f"Обрабатываем URL: {url}")
    host_id = get_host_id_for_url(hosts, urlparse(url).netloc)
    if not host_id:
        print(f"Не удалось найти host_id для URL: {url}")
        return None

    query_analytics = get_query_analytics(user_id, host_id, urlparse(url).path)
    average_ctr_per_position = calculate_average_ctr_per_position(query_analytics)
    if query_analytics:
        formatted_data = format_query_analytics(query_analytics)
        results = []
        for data in formatted_data:
            forecast_1, forecast_3, forecast_5 = forecast_clicks(data['average_position'], average_ctr_per_position, data['total_demand'],  data['total_clicks'], 'ctr.csv')
            results.append({
                'URL': url,
                'Запрос': data['query'],
                'Показы': f"{data['total_impressions']}".replace('.', ','),
                'Клики': f"{data['total_clicks']}".replace('.', ','),
                'Ср. Позиция': f"{data['average_position']:.2f}".replace('.', ','),
                'Ср. CTR': f"{data['average_ctr']:.2f}".replace('.', ','),
                'Спрос': f"{data['total_demand']}".replace('.', ','),
                '% от Спроса': f"{data['percent_impressions_demand']}".replace('.', ','),
                'Прогноз кликов TOP-1': f"{forecast_1:.2f}".replace('.', ','),
                'Прогноз кликов TOP-3': f"{forecast_3:.2f}".replace('.', ','),
                'Прогноз кликов TOP-5': f"{forecast_5:.2f}".replace('.', ',')
            })
        save_results_to_csv(results, output_file, brands, stop_words)
    else:
        print(f"Не удалось получить аналитику запросов для URL: {url}")

    return host_id

def main():
    user_id = get_user_id()
    if not user_id:
        print("Не удалось получить user_id")
        return

    hosts = get_hosts_list(user_id)
    if not hosts:
        print("Не удалось получить список хостов")
        return

    urls = read_urls_from_file(URLS_FILE)
    if not urls:
        print(f"Файл {URLS_FILE} пуст или не найден")
        return
    
    # Загрузка брендовых названий и стоп-слов
    brands = load_brand_names('brand.txt')
    stop_words = load_stop_words('stopwords.txt')

    # Список для хранения всех данных CTR
    all_ctr_data = defaultdict(lambda: {'clicks': 0, 'impressions': 0})

    # Process each URL
    for url in urls:
        url = url.strip()
        if url:
            # Получение и сохранение среднего CTR по популярным запросам
            process_site_ctr(url, hosts, user_id, 'ctr.csv')
            host_id = process_url(url, hosts, user_id, OUTPUT_CSV, brands, stop_words)
            if host_id:
                query_analytics = get_query_analytics(user_id, host_id, urlparse(url).path)
                if query_analytics:
                    average_ctr_per_position = calculate_average_ctr_per_position(query_analytics)
                    for position, data in average_ctr_per_position.items():
                        all_ctr_data[position]['clicks'] += data['clicks']
                        all_ctr_data[position]['impressions'] += data['impressions']

if __name__ == "__main__":
    main()
    convert_csv_encoding('query_analytics.csv', 'query_analytics_cp1251.csv', from_encoding='utf-8', to_encoding='cp1251')
