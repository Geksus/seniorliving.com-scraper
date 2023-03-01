import random
import multiprocessing as mp

import openpyxl
import requests
import sqlite3
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from export import export

wb = openpyxl.load_workbook('zipcodestowork.xlsx')

# select the worksheet to read from
ws = wb.active  # replace with the name of your worksheet

# iterate over the column and append values to a list
zipcodes = []
for cell in ws['A']:
    zipcodes.append(cell.value)

headers = [
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/15.15063'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0'},
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/54.0 Safari/537.36'},
    {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8'},
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/15.15063'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; AS; rv:11.0) like Gecko'},
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; Trident/7.0; AS; rv:11.0) like Gecko'},
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; AS; rv:11.0) like Gecko'},
    {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/15.15063'},
]

BASE_URL = 'https://www.seniorliving.com'
SEARCH_URL = 'https://www.seniorliving.com/find-a-senior-living-community?distance%5Bdistance%5D=40&distance%5Bunit%5D=3959&distance%5Borigin%5D='
types = ['care_type=independent_living',
         '&care_type=assisted_living',
         '&care_type=alzheimers_care',
         '&care_type=continuing_care_retirement_community',
         'care_type=home_care',
         '&care_type=nursing_home'
         ]

session = requests.Session()


def scrape_locations(url):
    response = session.get(url, headers=random.choice(headers))
    soup = BeautifulSoup(response.text, 'html.parser')
    if all_locations_div := soup.find('div', id='views-bootstrap-grid-1'):
        all_locations = all_locations_div.find_all('a')
        return [BASE_URL + i['href'] for i in all_locations]


def scrape_data(url):
    zipcode = ''
    care_name = ''
    type_of_care = ''
    title = ''
    address = ''
    description = ''
    contact_information = ''
    website = ''
    payment_type = ''
    response = session.get(url, headers=random.choice(headers))
    soup = BeautifulSoup(response.text, 'html.parser')
    try:
        zipcode = soup.find('span', class_='postal-code').text
    except Exception:
        zipcode = ''

    try:
        care_name = soup.find('h1', class_='page-header').text
    except Exception:
        care_name = ''

    try:
        type_of_care = \
            soup.find('div',
                      class_='field field-name-field-type-of-care field-type-list-text field-label-hidden').text.split(
                ',')[-1]
    except Exception:
        type_of_care = ''

    try:
        title = soup.find('div',
                          class_='field field-name-field-type-of-care field-type-list-text field-label-hidden').text
    except Exception:
        title = ''

    try:
        street = soup.find('div',
                           class_='field field-name-field-address field-type-addressfield field-label-hidden').find(
            'div', class_='thoroughfare').text
        city = soup.find('div',
                         class_='field field-name-field-address field-type-addressfield field-label-hidden').find(
            'span', class_='locality').text
        state = soup.find('div',
                          class_='field field-name-field-address field-type-addressfield field-label-hidden').find(
            'span', class_='state').text
        address = f"{street}, {city}, {state}"

    except Exception:
        try:
            street = soup.find('div',
                               class_='field field-name-field-address-caring field-type-addressfield field-label-hidden').find(
                'div', class_='thoroughfare').text
            city = soup.find('div',
                             class_='field field-name-field-address-caring field-type-addressfield field-label-hidden').find(
                'span', class_='locality').text
            state = soup.find('div',
                              class_='field field-name-field-address-caring field-type-addressfield field-label-hidden').find(
                'span', class_='state').text
            address = f"{street}, {city}, {state}"
        except Exception:
            address = ''
        address = ''

    try:
        description = soup.find('div',
                                class_='field field-name-body field-type-text-with-summary field-label-hidden').text
    except Exception:
        description = ''

    try:
        contact_information = f"{soup.find('div', class_='field field-name-field-contact-name field-type-text field-label-hidden').text}, " \
                              f"{soup.find('div', class_='field field-name-field-contact-title field-type-text field-label-hidden').text}, " \
                              f"{soup.find('div', class_='field field-name-field-phone field-type-phone field-label-hidden').text}, " \
                              f"{soup.find('div', class_='field field-name-field-email field-type-email field-label-hidden').find('a')['href']}"
    except Exception:
        contact_information = ''

    try:
        website = \
            soup.find('div', class_='field field-name-field-website field-type-link-field field-label-above').find('a')[
                'href']
    except Exception:
        website = ''

    try:
        payment_type = ', '.join([i.text for i in soup.find('div',
                                                            class_='field field-name-field-payment-type field-type-list-text field-label-above').find_all(
            'li')])
    except Exception:
        payment_type = ''

    search_zipcode = ''

    return {
        'search_zipcode': search_zipcode,
        'zipcode': zipcode,
        'care_name': care_name,
        'type_of_care': type_of_care,
        'title': title,
        'address': address,
        'description': description,
        'contact_information': contact_information,
        'website': website,
        'payment_type': payment_type
    }


def scrape_location_zipcode_type(zipcode, type):
    urls = []
    for i in range(10):
        try:
            urls.extend(scrape_locations(f'{SEARCH_URL}{str(zipcode)}{type}&page={str(i)}'))
        except TypeError:
            break
    return urls


def scrape_location_zipcode_types(zipcode, types):
    urls = []
    for type in types:
        urls.extend(scrape_location_zipcode_type(zipcode, type))
    return urls


zipcodes = zipcodes[33:]

if __name__ == '__main__':
    conn = sqlite3.connect('senior_living.db')
    c = conn.cursor()
    c.execute(
        'CREATE TABLE IF NOT EXISTS data (search_zipcode TEXT, zipcode TEXT, care_name TEXT, type_of_care TEXT, title TEXT, '
        'address TEXT, description TEXT, contact_information TEXT, website TEXT, payment_type TEXT)')
    # set the PRAGMA settings to increase the limit of the database to 1000000 entries
    c.execute('PRAGMA page_size=4096;')
    c.execute('PRAGMA cache_size=1000000;')
    c.execute('PRAGMA temp_store=MEMORY;')
    c.execute('PRAGMA max_page_count=1000000;')

    num_processes = 8

    pool = mp.Pool(processes=num_processes)

    for zipcode in zipcodes:
        print(f"Zipcode {zipcode}")
        print(f"Zipcodes left: {len(zipcodes) - zipcodes.index(zipcode)}")
        urls = pool.apply(scrape_location_zipcode_types, args=(zipcode, types))
        print(urls)
        with ThreadPoolExecutor(16) as executor:
            try:
                futures = [executor.submit(scrape_data, url) for url in sorted(urls)]
            except TypeError:
                pass
        for future in as_completed(futures):
            page_data = future.result()
            page_data['search_zipcode'] = zipcode
            c.execute(
                'INSERT INTO data (search_zipcode, zipcode, care_name, type_of_care, title, address, description, contact_information, website, payment_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (page_data['search_zipcode'], page_data['zipcode'], page_data['care_name'], page_data['type_of_care'],
                 page_data['title'],
                 page_data['address'], page_data['description'], page_data['contact_information'],
                 page_data['website'],
                 page_data['payment_type']))
            conn.commit()

    conn.close()

    export()
