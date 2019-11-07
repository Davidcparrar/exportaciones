import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import urllib.request
import requests, zipfile, io

url = "https://www.dian.gov.co/dian/cifras/Paginas/registrodeclaracionesimpoexpo.aspx"
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.binary_location = 'venv/chromedriver-Linux64'
driver = webdriver.Firefox()
driver.get(url)
time.sleep(10)

groups_click = driver.find_elements_by_xpath(".//td[@class='ms-gb']//a[contains(@onclick, 'ExpCollGroup')]")

for group in groups_click[0:2]:
    #print('in')
    #print(group.get_attribute('innerHTML'))
    group.click()
    time.sleep(2)

groups_click = driver.find_elements_by_xpath(".//td[@class='ms-gb2']//a[contains(@onclick, 'ExpCollGroup')]")

for group in groups_click:
    try:
        group.click()
    except:
        #print(group.get_attribute('innerHTML'))
        pass

    time.sleep(2)

page = driver.page_source
driver.quit()
soup = BeautifulSoup(page, 'html.parser')
container = soup.find_all('a', attrs={
     'class':'ms-listlink'}, href=True)

for a in container:
    file_path = a['href']
    print(file_path)
    name = file_path.split('/')[-1]
    file_path = 'https://www.dian.gov.co'+file_path
    print('Downloading: {}'.format(name))
    #urllib.request.urlretrieve(file_path, '/home/davidcparrar/Documents/Resources/exportaciones/'+ name)
    r = requests.get(file_path)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall('/home/davidcparrar/Documents/Resources/exportaciones/')
