# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import time

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
#driver = webdriver.Chrome(ChromeDriverManager().install())

path_json_fotocasa = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/fotocasa.json'
path_json_idealista = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/idealista_news.json'

try:
    for path_json in [path_json_fotocasa, path_json_idealista][-1:]:

        df_json = pd.read_json(path_json, orient='records', lines=True, encoding="utf8")
        df_json_finish = pd.DataFrame()
        results = []
        has_scraper = True

        for idx, x in df_json.iterrows():
            has_scraper = True
            if x.loc['Status']:
                if x.loc['Status'] == 'OUT':
                    print(idx, 'OUT')
                    has_scraper = False
            else:
                x['Status'] = ''

            if x.loc['Date_status']:
                if x.loc['Date_status'] >= str(datetime.date.today() - datetime.timedelta(days=7)):
                    print(idx, 'Fecha reciente')
                    has_scraper = False
            else:
                x['Date_status'] = '0'

            if has_scraper:
                ch_exe = r'C:/Users/aasensio/.wdm/drivers/chromedriver/win32/86.0.4240.22/chromedriver.exe'
                options = webdriver.ChromeOptions()
                options.add_argument("--disable-blink-features")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument('--disable-gpu')
                options.add_argument('--no-sandbox')
                driver = webdriver.Chrome(options=options, executable_path=ch_exe)
                driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                    "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    })
                  """
                })

                print(x.loc['Url'])
                driver.get(x.loc['Url'])
                driver.implicitly_wait(20)
                time.sleep(2)

                if path_json.split('/')[-1].split('.')[0] == 'Nfotocasa':
                    classes = ['re-DetailHeader-priceContainer', 're-DetailHeader-features', 're-DetailHeader-propertyTitle'
                        , 'fc-DetailDescription', 're-DetailFeaturesList-feature', 're-DetailMap-address', 're-DetailEnergyCertificate-itemList']
                    campos = ['precio', 'tamaño', 'titulo', 'descripcion', 'tipo', 'dirrecion', 'certf energetico']
                    results_dict = {}
                    for campo, classe in zip(campos, classes):
                        try:
                            print('\n', classe)
                            text1 = driver.find_element_by_class_name(classe)
                            results_dict[campo]: text1.text
                        except:
                            print('\nNot Mach:', classe)
                            pass

                if path_json.split('/')[-1].split('.')[0] == 'Nidealista_news':
                    data_web = {}
                    for idx, classe in enumerate(['h2-simulated', 'main-info__title-block', 'info-data-price',
                        'info-features', 'comment', 'details-property-feature-one', 'details-property-feature-two',
                        'price-feature clearfix', 'clearfix', 'professional-name']):
                        try:
                            print(classe)
                            text1 = driver.find_element_by_class_name(classe)
                            print(text1.text)
                            data_web[idx] = text1.text
                        except:
                            pass

                #driver.close()
                #print(results_dict)
                #results = results.append(results_dict)

                status = 'Ok' if driver.current_url == x.loc['Url'] else 'OUT'
                x['Status'] = status
                x['Date_status'] = str(datetime.date.today())
                #x = x.append(pd.Series([status, str(datetime.date.today())], index=['Status', 'Date_status']))

                #print(x.to_frame().T)

                driver.close()

            # Check and Add house
            if df_json_finish.empty:
                df_json_finish = x.to_frame().T
            else:
                df_json_finish = df_json_finish.append(x.to_frame().T, ignore_index=True)

            print(x.to_frame().T[['Status', 'Date_status']])

        #print(results)

        #pd.DataFrame(results).to_json(path_json.replace('.json', '_scrap.json'), orient='records', lines=True)
        import json

        df_json_finish.to_json(path_json.replace('.json', '2.json'), orient='records', lines=True)
        #json.dumps(results).to_json(path_json.replace('.json', '_scrap2.json'), orient='records', lines=True)

except Exception as error:
    driver.close()
    print(error)

print('FIN')
