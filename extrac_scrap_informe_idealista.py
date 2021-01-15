import re, collections, itertools, json
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

# URL
url = 'https://www.idealista.com/sala-de-prensa/informes-precio-vivienda/venta/madrid-comunidad/madrid-provincia/madrid/'
executable_path = r'C:\Users\aasensio\.wdm\drivers\chromedriver\win32\87.0.4280.88\chromedriver.exe'


# guardar-paginaweb.py
try:
    import urllib.request, urllib.error, urllib.parse
    respuesta = urllib.request.urlopen(url)
    contenidoWeb = respuesta.read()
    print(contenidoWeb)
    f = open('Evolución del precio de la vivienda en venta en Madrid — idealista.html', 'wb')
    f.write(contenidoWeb)
    f.close
except Exception as error:
    print('Error' , error)


#driver = webdriver.Chrome(ChromeDriverManager().install())
options = webdriver.ChromeOptions()
# options.add_experimental_option('useAutomationExtension', False)
# options.add_experimental_option("excludeSwitches", ['enable-automation'])
options.add_argument("--disable-blink-features")
options.add_argument("--disable-blink-features=AutomationControlled")
driver = webdriver.Chrome(options=options, executable_path=executable_path)
driver.implicitly_wait(10)

try:
    driver.get(url)
    driver.implicitly_wait(10)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    print(type(soup))
    all_data = []
    #soup = BeautifulSoup(driver.page_source,"lxml")
    while True:
        data = soup.find_all('div', {'class': 'table__scroll'})
        print(data)
        all_data += data
        print(len(data))

        try:
            #driver.find_element_by_xpath('//a[contains(.,"Siguiente")]').click()
            #soup = BeautifulSoup(driver.page_source, 'html.parser')
            break
        except Exception: break

    #homes = soup(driver.page_source, 'html.parser').find_all('div', {'class':'item-info-container'})
    try:
        for x in all_data:
            list_houses2 = x.find_all('a')

            print(list_houses2)

            print(x.find_all('a')[0].title)
            break

        list_title = all_data.a
        print(type(list_title))
        print(all_data[0].title)

    except Exception as error:
        print('Error', error)
        pass
    try:
        try:
            path_write_json = 'C:/Users/aasensio/Downloads/idealista.json'
            #homes[0:1].to_json(path_write_json, orient='records', lines=True)
        except Exception as error:
            print('ERROR', error)

    except:
        pass

    try:
        results = [i.find('div', {'class':re.compile(r'price\-row')}).text for i in all_data]
        print(results[0:1])
    except:

        pass

    price = collections.namedtuple('price', ['original', 'current', 'drop'])
    try:
        print('collections.namedtuple', price)
    except:

        pass
except:
    pass

    # driver.close()

