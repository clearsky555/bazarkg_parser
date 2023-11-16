from datetime import datetime
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
import csv

from config import CSV_FILE_NAME


def get_html(URL): # делать запрос по ссылке и возвращать html код этой страницы
    response = requests.get(URL)
    return response.text


def get_posts_links(html):
    links = []
    soup = BeautifulSoup(html, "html.parser")
    table_data = soup.find("div", {"class":"listings-wrapper"})
    data = table_data.find_all("div", {"class":"listing row-5"})
    for p in data:
        href = p.find('a').get('href')
        full_url = 'https://www.bazar.kg' + href
        links.append(full_url)
    return links


def get_detail_post(html, post_url):
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('div', {'class':'block-main details'})

    title = content.find('h1').text.strip()
    som = content.find('div', {'class': 'block-sub price'}).find('span', {'class': 'main'}).text
    dollar = content.find('div', {'class': 'block-sub price'}).find('span', {'class': 'sub'}).text
    city = content.find('div', {'class': 'adress'}).text.strip()
    phone = content.find('div', {'class': 'number-holder'}).text.strip()

    data = {
        'title': title,
        'som':  som,
        'dollar': dollar,
        'mobile': phone,
        'city': city,
        'link': post_url
    }
    print(data)
    return data


def get_lp_number(html):
    soup = BeautifulSoup(html, 'html.parser')
    content = soup.find('nav')
    ul = content.find('ul', {'class':'pagination'})
    lp = ul.find_all('a', {'class':'page-link'})[-2].text
    return int(lp)


def write_data(data):
    # Запись данных
    with open(CSV_FILE_NAME, 'a', encoding='utf-8') as file:
        headers = ['title', 'mobile', 'dollar', 'som', 'link', 'city']
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writerow(data)


def write_header_csv():
    # Запись заголовка
    with open(CSV_FILE_NAME, 'w', encoding='utf-8') as file:
        headers = ['title', 'mobile', 'dollar', 'som', 'link', 'city']
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()


def get_parse_page(page):
    URL_MAIN = 'https://www.bazar.kg/'
    filter = 'kyrgyzstan/elektronika?'
    FULL_URL = URL_MAIN + filter
    print(f'Парсинг страницы: {page}')
    FULL_URL += f'page={page}'
    html = get_html(FULL_URL)
    post_links = get_posts_links(html)
    for link in post_links:
        post_html = get_html(link)
        post_data = get_detail_post(post_html, post_url=link)
        write_data(data=post_data)


def main():
    write_header_csv()
    start = datetime.now()
    URL_MAIN = 'https://www.bazar.kg/'
    filter = 'kyrgyzstan/elektronika?'
    FULL_URL = URL_MAIN + filter
    html = get_html(FULL_URL)
    last_page = get_lp_number(html)
    with Pool(5) as p:
        p.map(get_parse_page, range(1, last_page+1))

    end = datetime.now()
    print('Время выполнения: ', end-start)


if __name__ == '__main__':
    main()

