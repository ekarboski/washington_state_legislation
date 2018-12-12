from selenium.webdriver import (Chrome, Firefox)
import pandas as pd
import urllib
from bs4 import BeautifulSoup


def scrape_bill_topic_table(year):
    """Scrape data from apps.leg.wa.gov/billsbytopic and organize into a dataframe 
       
    Args:
        year (str): for example '2015' 
    
    Returns:
        pandas dataframe
    """
    
    browser = Firefox()
    browser.get('http://apps.leg.wa.gov/billsbytopic/Results.aspx?year={}'.format(year))

    table = browser.find_elements_by_css_selector('div#divContent table')[1]
    html = table.get_attribute('innerHTML')
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.select('tr')
    data = []
    heading = None
    
    for i, row in enumerate(rows):
        row_data = {}
        tds = row.select('td')
        
        if not tds: 
            continue
        if tds[0].attrs.get('width') != '5%':
            heading = row.select_one('td a').text.strip()
            continue
            
        row_data['bill_topic'] = heading
        row_data['bill_topic_expanded'] = tds[1].text.partition(':')[0]
        bill_ids = [a.text for a in tds[1].select('a')]

        for bill_id in bill_ids:
            row_data['bill_id'] = bill_id
            data.append(row_data)

    return pd.DataFrame(data)


def scrape_bill_url(url):
    """Scrape all text from the bill url and put into one string.
    
    Args:
        url (str): url provided by bill_api
        
    Returns:
        text (str): text of bill    
        """

    url = url.replace(' ', '%20')
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, "lxml")
    for script in soup(["script", "style"]):
        script.extract()
    raw_text = soup.get_text()
    lines = [l.strip() for l in raw_text.splitlines()]
    phrases = [phrase.strip() for line in lines for phrase in line.split("  ")]
    text = '\n'.join([phrase for phrase in phrases])
    return text