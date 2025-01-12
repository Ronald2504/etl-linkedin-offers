import requests
from prefect import task
from bs4 import BeautifulSoup

LINKEDIN_URL = 'https://www.linkedin.com/jobs/search/?geoId=102927786&keywords=python'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

@task(name="Extract")
def task_extract():
    url = requests.get(LINKEDIN_URL,headers=HEADERS)

    if(url.status_code == 200):
        html = BeautifulSoup(url.text,'html.parser')
        ul_offers = html.find('ul',{'class':'jobs-search__results-list'})
        li_offers = ul_offers.find_all('li')

        offer_list = []
        
        for offer in li_offers:
            offer_title = offer.find('h3',{'class':'base-search-card__title'})
            offer_location = offer.find('span',{'class':'job-search-card__location'})
            offer_url = offer.find('a')
            offer_company = offer.find('a',{'class':'hidden-nested-link'})
            offer_date = offer.find('time',{'class':'job-search-card__listdate'})
            
            title = offer_title.get_text().strip() if offer_title else ''
            location = offer_location.get_text().strip() if offer_location else ''
            url_value = offer_url['href'].strip() if offer_url else ''
            company = offer_company.get_text().strip() if offer_company else ''
            date_value = offer_date['datetime'].strip() if offer_date else None

            """print(f'titulo : {offer_title.get_text().strip()}')
            print(f'ubicacion : {offer_location.get_text().strip()}')
            print(f'empresa : {company}')
            print(f'url:{offer_url}')
            print(f'fecha : {offer_date_value}')"""
            offer_list.append((title,location,company,date_value,url_value))
            
        return offer_list
    else:
        print(f"error : {url.status_code}")
