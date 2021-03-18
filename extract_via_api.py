import json
import base64
import requests
import datetime
import pandas as pd
import urllib.parse


# FUNCTIONS
def get_oauth_token():
    url_token = "https://api.idealista.com/oauth/token"
    apikey = 'xxd1f49llzv0eglzg0w8n8m0adxxifxw'     # sent by idealista
    secret = 'rSahNvEUxg29'                         # sent by idealista
    # apikey = '5rpvqg76ub95zxl5pvm5eyvk5xvyxudq'     # sent by idealista tonytroker
    # secret = 'RrFp0MSpTBfF'                         # sent by idealista tonytroker
    keysecret = apikey + ':' + secret
    auth = base64.b64encode(keysecret.encode("utf-8"))
    content_type = 'application/x-www-form-urlencoded;charset=UTF-8'
    headers = {'Content-Type': content_type, 'Authorization': 'Basic ' + auth.decode('utf-8')}
    params = urllib.parse.urlencode({'grant_type': 'client_credentials'})
    content = requests.post(url_token, headers=headers, params=params)
    bearer_token = json.loads(content.text)
    print(bearer_token)
    return bearer_token['access_token']


def search_api(token, url_search):
    content_req = ''
    try:
        headers = {'Content-Type': 'Content-Type: multipart/form-data;', 'Authorization': 'Bearer ' + token}
        content_req = requests.post(url_search, headers=headers)
        result = json.loads(content_req.text)
        return result
    except Exception as error_search_api:
        try:
            print('Error', error_search_api)
            print(content_req.content)
            raise
        except Exception as error_search_api2:
            print(error_search_api2)
            raise


# VARIABLES
# PATH = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/Madrid/API/'
PATH = 'C:/Users/aasensio/Google Drive/Personal/Python/Data_inmob/API/raw/'
hora = str(datetime.datetime.today().strftime("%Y-%m-%d_%H-%M"))
dia = str(datetime.date.today())

country = 'es'
locale = 'es'
operation = 'sale'
propertyType = 'homes'
flat = 'True'
preservation = 'good'
center, distance = '0', 0
# center, zona = '40.4167,-3.70325', 'centro'              # Puerta del sol, Madrid
# center, zona, distance = '40.347466,-3.6996134', 'villaverde', 10000    # Villaverde, Madrid
# center, zona = '40.4057479,-3.6920406', 'atocha'         # Estacion de Atocha
# locationId = "0-EU-ES-28-07-001-079-17-003"     # locationLevel":"8"    Madrid, Villaverde, Butarque
# locationId = "0-EU-ES-28-07-001-079-17-005"     # locationLevel":"8"    Madrid, Villaverde, Los Angeles
# locationId, zona = "0-EU-ES-28-07-001-079-17", 'villaverde'       # locationLevel":"7"    Madrid, Villaverde
# locationId, zona = "0-EU-ES-28-07-001-079-18", 'villa_vallecas'   # locationLevel":"7"    Madrid, Villa de Vallecas
# locationId, zona = "0-EU-ES-28-07-001-079-13", 'pte_de_vallecas'  # locationLevel":"7     Madrid, Puente de Vallecas
# locationId, zona = "0-EU-ES-28-07-001-079-11", 'carabanchel'      # locationLeve: 7       Madrid, Carabanchel
# locationId, zona = "0-EU-ES-28-07-001-079-12", 'usera             # locationLeve: 7       Madrid, Usera
lugares = [('0-EU-ES-28-07-001-079-17', 'villaverde'), ('0-EU-ES-28-07-001-079-13', 'pte_de_vallecas'),
           ('0-EU-ES-28-07-001-079-11', 'carabanchel'), ('0-EU-ES-28-07-001-079-18', 'villa_vallecas'),
           ('0-EU-ES-28-07-001-079-12', 'usera'), ("0-EU-ES-28-07-001-079", "madrid")]
minPrice =  60000
maxPrice = 115000
sinceDate = 'M'  # W:last week, M: last month, T:last day (for rent except rooms), Y: last 2 days (sale and rooms)
order = 'priceDown'
sort = 'desc'
url_api = 'https://api.idealista.com/3.5/es/search??'
url_0 = f'{url_api}locale={locale}&maxItems=50&operation={operation}&country={country}&propertyType={propertyType}' + \
        f'&sort={sort}&order={order}&language=es&flat=True'  # &preservation={preservation}'
limit = 40


if __name__ == '__main__':
    try:
        # 1/0
        for locationId, zona in lugares[-1:]:
            oauth = get_oauth_token()

            # Dato global
            url_gb = url_0 + f'&locationId={locationId}'  # url por localizacion
            total = search_api(oauth, url_gb)['total']

            df_tot = pd.DataFrame()
            for i in range(1, limit + 1):
                print('Iter num: ', i)

                price = f'&minPrice={minPrice}&maxPrice={maxPrice}'
                url_01 = url_0 + price + f'&center={center}&distance={distance}&numPage={i}'  # url por coordenadas GPS
                url_02 = url_0 + price + f'&locationId={locationId}&numPage={i}'              # url por localizacion
                url_03 = url_02 + f'&sinceDate={sinceDate}'                                   # url02 en el último mes

                # Lanzar API
                results = search_api(oauth, url_02)

                # Leer Resultados
                if i == 1:
                    print(url_02)
                    print('total:', results['total'])
                    print('totalPages:', results['totalPages'], '\n')
                df = pd.DataFrame.from_dict(results['elementList'])
                if df.shape[0] == 0:
                    break

                # Salvar Resultados Intermedios
                path_save = PATH + '/idealista_%s_%s_part_%s.json' % (zona, dia, i)
                df.to_json(path_save, orient='records', lines=True)
                df_tot = df_tot.append(df, ignore_index=True, sort=True)

                # Fin de las iteraciones
                if i == results['totalPages']:
                    break

            # Salvar resultados Totales por zona
            path_save = PATH + '/idealista_api_%s_%s.json' % (zona, dia)
            df_tot['Total'] = total
            df_tot.to_json(path_save, orient='records', lines=True)

    except Exception as error:
        print('Error', error)
        raise
