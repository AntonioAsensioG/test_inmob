import datetime
import pandas as pd
from os import listdir, path

# CONFIG LIBRERIAS Y MODULOS
pd.set_option('display.width', 280)
pd.set_option('display.max_colwidth', 25)
pd.set_option('display.max_columns', 32)
pd.set_option('display.max_rows', 10)

# FUNCTIONS


# VARIABLES
path_madrid = path.dirname(path.abspath(__file__)) + '/Data/Madrid/'

# CARGAR HISTORICO OFERTA IDEALISTA
try:
    1/0
    data_oferta = pd.read_csv(path_madrid + 'idealista_oferta.csv')
except:
    data_oferta = pd.DataFrame()

# CARGAR DATA IDEALISTA DE OFERTA
files = sorted([f for f in listdir(path_madrid + 'API/') if 'idealista_api_madrid_60K_100K' in f])
print(files)

data = pd.DataFrame()
df_last = pd.DataFrame()
for i in files[-2:]:
    print('\n', i.upper())
    try:
        # Cargar datos
        df = pd.read_json(path_madrid + 'API/' + i, orient='records', lines=True)
        df = df.rename(columns={'propertyCode': 'Id', 'newDevelopment': 'Is_New', 'hasLift': 'Has_Lift',
                                'hasParkingSpace': 'Has_Parking', 'priceByArea': 'Price_m2', 'exterior': 'Is_Ext',
                                'bathrooms': 'Bathrooms', 'rooms': 'Rooms', 'price': 'Price'})

        # Convertir dict de las columnas en multiples columnas (se generarn NA)
        df = df.join(df['detailedType'].apply(pd.Series))
        df = df.rename(columns={'typology': 'Type', 'subTypology': 'Sub_Type'})
        df['Sub_Type'] = df['Sub_Type'].combine_first(df['Type'])

        df = df.join(df['parkingSpace'].apply(pd.Series))
        df = df.rename(columns={'hasParkingSpace': 'Has_Parking'})
        df['Has_Parking'] = df['Has_Parking'].fillna(False)

        df = df.join(df['suggestedTexts'].apply(pd.Series))

        df['Has_Lift'] = df['Has_Lift'].replace({0: False, 1: True}).fillna(False)

        # Filtrar Pisos y en buen estado
        df = df.loc[(df['Type'] == 'flat') & (df['status'] == 'good')]

        # Limpiar acentos
        trans = str.maketrans('áãàâäéëêèíìïîóõòöôúüùûÁÉÍÓÚ', 'aaaaaeeeeiiiiooooouuuuAEIOU')
        for x in ['neighborhood', 'address', 'district', 'title', 'subtitle']:
            df[x] = df[x].apply(lambda t: t.translate(trans))

        # Eliminar cols innecesarias y quitar duplicados
        for x in ['country', 'thumbnail', 'has360', 'has3DTour', 'hasVideo', 'hasStaging', 'topNewDevelopment',
                  'newDevelopmentFinished', 'showAddress', 'detailedType', 'parkingSpace', 'suggestedTexts', 'hasPlan',
                  'propertyType']:
            if x in df:
                df.drop(x, axis=1, inplace=True)
        df = df.drop_duplicates()

        # Ver duplicados
        # df_dupli = pd.DataFrame(df['id'].value_counts())
        # print(df_dupli.loc[df_dupli['id'] > 1])
        # print(df.loc[df['id'].isin(['92193637', '92050796'])])

        # Agrupar por intervalos el tamaño, y la planta agrupar en bajo o intermedia
        df['Size_Interval'] = pd.cut(df['size'], pd.interval_range(start=0, end=df['size'].max() + 1, freq=10))
        df['Floor'] = df['floor'].apply(lambda f: f if f == 'bj' else 'alt')

        # Crear Categoria
        df['Category'] = df['Type'].str[0:2].str.capitalize()
        df['Category'] = df['Category'] + df['Size_Interval'].apply(lambda a: int(a.mid)).astype('str')
        df['Category'] = df['Category'] + df['Is_Ext'].apply(lambda a: 'Ex' if a else 'In')
        df['Category'] = df['Category'] + df['Bathrooms'].apply(lambda a: str(a) + 'b' if a else '')
        df['Category'] = df['Category'] + df['Has_Lift'].apply(lambda a: 'Lf' if a else '')
        df['Category'] = df['Category'] + df['Floor'].apply(lambda a: 'Bj' if a == 'bj' else '')
        df['Category'] = df['Category'] + df['Is_New'].apply(lambda a: 'Nw' if a else '')
        df['Size_Interval'] = df['Size_Interval'].astype('str')

        # Calculo columnas: fecha, precio medio, ...
        fecha = datetime.datetime.strptime(i.split('_')[5].split('.')[0], '%Y-%m-%d')
        precio_median_m2 = df['Price_m2'].median()
        precio_medio_m2 = df['Price_m2'].mean()
        precio_median = df['Price'].median()
        precio_medio = df['Price'].mean()

        # Calculo de variaciones de precios
        if not df_last.empty:
            df = df_last[['Id', 'Price']].rename(columns={'Price': 'Price_old'}).merge(df, on='Id', how='right')
            df['Diff'] = df[['Price', 'Price_old']].apply(lambda a: a['Price_old'] - a['Price_old'], axis=1)
        else:
            df['Diff'] = 0
            df['Price_old'] = df['Price']
        df[['Price_Up', 'Price_Down', 'Price_Keep']] = df['Diff']\
            .apply(lambda a: pd.Series({'Price_Up': a > 0, 'Price_Down': a < 0, 'Price_Keep': a == 0})) * 1
        df['Diff'] = df['Diff'].fillna(0)

        # AGRUPAR OFERTA
        # Identificar typo de piso más ofertado por zonas (vecindario)
        # cols = ['Type', 'Is_New', 'Is_Ext', 'Has_Lift', 'Has_Parking', 'Bathrooms', 'Size_Interval', 'Floor']
        cols = ['neighborhood', 'Category']
        print(df.columns)
        df_ng = df[cols + ['Price_m2', 'Price', 'Id', 'Rooms', 'Diff', 'Price_Up', 'Price_Down', 'Price_Keep']]\
            .groupby(cols)\
            .agg(Offers=('Id', 'count'), Price_m2_med=('Price_m2', 'median'), Price_m2_mean=('Price_m2', 'mean'),
                 Price_med=('Price', 'median'), Price_mean=('Price', 'mean'), Rooms_med=('Rooms', 'median'),
                 Diff=('Diff', 'mean'))\
            .reset_index().sort_values('Offers', ascending=False)
        df_ng = df_ng.rename(columns={'neighborhood': 'Zone'})
        df_ng['Zone_Level'] = 'neighborhood'
        df_ng['Price_m2_mean'] = df_ng['Price_m2_mean'].astype(int)
        df_ng['Price_m2_med'] = df_ng['Price_m2_med'].astype(int)
        df_ng['Price_mean'] = df_ng['Price_mean'].astype(int)
        df_ng['Price_med'] = df_ng['Price_med'].astype(int)
        df_ng['Year'] = int(fecha.year)
        df_ng['Month'] = 'M{:0>2}'.format(fecha.month)
        df_ng['Week'] = 'W{:0>2}'.format(fecha.isocalendar()[1])

        # Identificar typo de piso más ofertado por zonas (districto)
        cols = ['district', 'Category']
        df_ds = df[cols + ['Price_m2', 'Price', 'Id', 'Rooms', 'Diff', 'Price_Up', 'Price_Down', 'Price_Keep']]\
            .groupby(cols)\
            .agg(Offers=('Id', 'count'), Price_m2_med=('Price_m2', 'median'), Price_m2_mean=('Price_m2', 'mean'),
                 Price_med=('Price', 'median'), Price_mean=('Price', 'mean'), Rooms_med=('Rooms', 'median'),
                 Price_Up=('Price_Up', 'sum'), Price_Down=('Price_Down', 'sum'), Price_Keep=('Price_Keep', 'sum'),
                 Diff=('Diff', 'mean'))\
            .reset_index().sort_values('Offers', ascending=False)
        df_ds = df_ds.rename(columns={'district': 'Zone'})
        df_ds['Zone_Level'] = 'district'
        df_ds['Price_m2_mean'] = df_ds['Price_m2_mean'].astype(int)
        df_ds['Price_m2_med'] = df_ds['Price_m2_med'].astype(int)
        df_ds['Price_mean'] = df_ds['Price_mean'].astype(int)
        df_ds['Price_med'] = df_ds['Price_med'].astype(int)
        df_ds['Year'] = int(fecha.year)
        df_ds['Month'] = 'M{:0>2}'.format(fecha.month)
        df_ds['Week'] = 'W{:0>2}'.format(fecha.isocalendar()[1])

        # Entradas y Salidas de ofertas
        if not df_last.empty:
            repetidos = df.loc[df['Id'].isin(df_last['Id'])][['Id']].sort_values('Id').shape[0]
            nuevos = df.loc[~df['Id'].isin(df_last['Id'])][['Id']].sort_values('Id').shape[0]
            salidas = df_last.loc[~df_last['Id'].isin(df['Id'])][['Id']].sort_values('Id').shape[0]
            entradas_pct = round(nuevos / df.shape[0] * 100, 2)
            salidas_pct = round(salidas / df_last.shape[0] * 100, 2)
            repiten_pct = round(repetidos / df.shape[0] * 100, 2)
        else:
            repetidos = nuevos = salidas = entradas_pct = salidas_pct = repiten_pct = 0

        # Generar filas para el data agregado de oferta
        oferta = {'Zone': 'Madrid', 'Zone_Level': 'municipality', 'Zone_Catg': 'Madrid', 'Offers': df.shape[0],
                  'Year': int(fecha.year), 'Month': 'M{:0>2}'.format(fecha.month),
                  'Week': 'W{:0>2}'.format(fecha.isocalendar()[1]),
                  'Price_m2_mean': int(precio_medio_m2), 'Price_m2_med': int(precio_median_m2),
                  'Price_mean': int(precio_medio), 'Price_med': int(precio_median),
                  'Inputs': nuevos, 'Outputs': salidas, 'Keep': repetidos,
                  'Inputs_pct': entradas_pct, 'Outputs_pct': salidas_pct, 'Keep_pct': repiten_pct,
                  'Price_Up': df['Price_Up'].sum(), 'Price_Down': df['Price_Down'].sum(),
                  'Price_Keep': df['Price_Keep'].sum(), 'Diff': df['Diff'].mean()}
        oferta = df_ng.append(pd.DataFrame([oferta]), ignore_index=True).drop_duplicates()
        oferta = oferta.append(df_ds, ignore_index=True).drop_duplicates()

        # Unir con loas datos de otras semanas
        data_oferta = data_oferta.append(oferta, ignore_index=True).drop_duplicates()

        # Guardar resultados
        data_oferta.to_csv(path_madrid + 'idealista_oferta.csv', index=False, float_format='%.2f')

        # Guardar el último data de oferta
        if not df_last.empty:
            df['latitude'] = df['latitude'].astype(str)
            df['longitude'] = df['longitude'].astype(str)
            df.to_csv(path_madrid + 'idealista_oferta_casas.csv', index=False, float_format='%.2f')

        # Guardar el último data para comparar
        df_last = df.copy()

    except Exception as error:
        print('ERROR', error)
        raise ValueError(error)
