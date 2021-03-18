import datetime
import numpy as np
import pandas as pd
from os import listdir

# CONFIG LIBRERIAS Y MODULOS
pd.set_option('display.width', 170)
pd.set_option('display.max_colwidth', 190)
pd.set_option('display.max_columns', 54)
pd.set_option('display.max_rows', 10)

# VARIABLES
fecha_null = str(datetime.datetime(1, 1, 1, 0, 0, 0))
path = 'C:/Users/aasensio/Google Drive/Personal/Python/Data_inmob/API/raw/'
path_etl = 'C:/Users/aasensio/Google Drive/Personal/Python/Data_inmob/API/etl/'
path_gmail = 'C:/Users/aasensio/Google Drive/Personal/Python/Data_inmob/GMAIL/etl/idealista_news.json'

# CARGAR DATA GMAIL Con datos de la descripcion
df_gmail = pd.read_json(path_gmail, orient='records', lines=True)
df_gmail['Fecha'] = df_gmail['Fecha'].fillna(fecha_null).replace(0.0, fecha_null)
df_gmail = df_gmail.sort_values('Fecha', ascending=False).drop_duplicates('Id', keep='first')

# Ficheros Disponibles
files = sorted([f for f in listdir(path) if 'idealista_api_madrid' in f])
print(files)

# CARGAR DATA IDEALISTA DE OFERTA de LAS 2 ÚLTIMAS SEMANAS y ETL de los datos
data = pd.DataFrame()
df_last: pd.DataFrame = pd.DataFrame([], columns=['Id', 'Price'])

for idx, i in enumerate(files, start=1):
    print('\n', i.upper())

    # Cargar datos
    df = pd.read_json(path + i, orient='records', lines=True)
    fecha = i.split('_')[3].split('.')[0]

    # Columnas renombrar como Capitalize
    for x in df.columns:
        df = df.rename(columns={x: x.capitalize()})

    df = df.rename(columns={'Propertycode': 'Id', 'Newdevelopment': 'Is_New', 'Haslift': 'Has_Lift',
                            'HasParkingSpace': 'Has_Parking', 'Pricebyarea': 'Price_m2', 'Exterior': 'Is_Ext'})

    # Convertir dict de las columnas en multiples columnas (se generan NA)
    df = df.join(df['Detailedtype'].apply(pd.Series))
    df = df.rename(columns={'typology': 'Type', 'subTypology': 'Sub_Type'})
    df['Sub_Type'] = df['Sub_Type'].combine_first(df['Type'])

    df = df.join(df['Parkingspace'].apply(pd.Series))
    df = df.rename(columns={'hasParkingSpace': 'Has_Parking'})
    df['Has_Parking'] = df['Has_Parking'].fillna(False).replace(to_replace=1.0, value=True)
    df['isParkingSpaceIncludedInPrice'] = df['isParkingSpaceIncludedInPrice'].fillna(False)

    if 'parkingSpacePrice' not in df:
        df['parkingSpacePrice'] = 0
    df['parkingSpacePrice'] = df['parkingSpacePrice'].fillna(0)
    df['Price'] = df['Price'] + df[['Has_Parking', 'isParkingSpaceIncludedInPrice', 'parkingSpacePrice']] \
        .apply(lambda row: 0 if not row['Has_Parking'] else 0
    if row['isParkingSpaceIncludedInPrice'] else row['parkingSpacePrice'], axis=1)

    df = df.join(df['Suggestedtexts'].apply(pd.Series))
    df = df.rename(columns={'subtitle': 'Subtitle', 'title': 'Title'})

    df['Has_Lift'] = df['Has_Lift'].replace({0: False, 1: True}).fillna(False)

    # Eliminar cols innecesarias y quitar duplicados
    for x in ['Country', 'Thumbnail', 'Has360', 'Has3dtour', 'Hasvideo', 'Hasstaging', 'Topnewdevelopment',
              'Newdevelopmentfinished', 'Showaddress', 'Detailedtype', 'Parkingspace', 'Suggestedtexts', 'Hasplan',
              'Propertytype', 'Numphotos', 'isParkingSpaceIncludedInPrice', 'parkingSpacePrice']:
        if x in df:
            df.drop(x, axis=1, inplace=True)
    df = df.drop_duplicates()

    # Filtrar Pisos, estado y precio
    df = df.loc[(df['Type'] == 'flat')]

    # Unir con los datos de gmail para obtener la descripcion
    df = df.merge(df_gmail[['Id', 'Descripcion']], on='Id', how='left')
    df['Descripcion'] = df['Descripcion'].fillna('').astype('str')

    # Limpiar acentos
    trans = str.maketrans('áãàâäéëêèíìïîóõòöôúüùûÁÉÍÓÚ', 'aaaaaeeeeiiiiooooouuuuAEIOU')
    for x in ['Neighborhood', 'Address', 'District', 'Title', 'Subtitle', 'Descripcion']:
        df[x] = df[x].apply(lambda t: t.translate(trans))
        df[x] = df[x].str.replace('\r', '', regex=False)

    # Extraer dato de interes de la description
    if not df.loc[df['Descripcion'].str.contains('utiles')][['Descripcion', 'Size']].empty:
        print(df.loc[df['Descripcion'].str.contains('utiles')][['Descripcion', 'Size']])
    df.loc[df['Descripcion'].str.contains('reformado'), 'Status'] = 'reformed'
    df_tmp = df.loc[df['Descripcion'].str.contains('utiles')]['Descripcion'] \
        .str.extract(r'(\d+).\d metros cuadrados utiles')[0]
    df.loc[df['Descripcion'].str.contains('utiles'), 'Size'] = df_tmp

    # Solventar Size nulos
    df.loc[df['Size'].isnull(), 'Size'] = df.loc[df['Size'].isnull()]['Price'] / df.loc[df['Size'].isnull()]['Price_m2']

    # Formato de las columnas
    df['Price'] = df['Price'].astype(int)
    df['Size'] = df['Size'].astype(int)
    df['Latitude'] = df['Latitude'].astype(str)
    df['Longitude'] = df['Longitude'].astype(str)
    df['Total'] = df['Total'].fillna(0).astype(int) if 'Total' in df else 0
    df['Date'] = str(fecha)  # datetime.datetime.strptime(fecha, '%Y-%m-%d')
    df['Status'] = df['Status'].fillna('good').replace('renew', 'bad')
    df.loc[df['Is_New'], 'Status'] = 'new'

    # Agrupar por intervalos el tamaño, y la planta agrupar en bajo o intermedia
    df['Size_Interval'] = pd.cut(df['Size'], pd.interval_range(start=0, end=df['Size'].max() + 10, freq=10))
    df['Price_Interval'] = pd.cut(df['Price'], pd.interval_range(start=0, end=df['Price'].max() + 2500, freq=2500))
    df['Price_Interval'] = df['Price_Interval'].astype(str)
    df['Floor_int'] = df['Floor'].apply(lambda f: f if f == 'bj' else 'alt')

    # Crear Categoria
    df['Category'] = df['Type'].str[0:2].str.capitalize()
    df['Category'] = df['Category'] + df['Size_Interval'].apply(lambda a: int(a.mid)).astype(str)
    df['Category'] = df['Category'] + df['Is_Ext'].apply(lambda a: 'Ex' if a else 'In')
    df['Category'] = df['Category'] + df['Bathrooms'].apply(lambda a: str(a) + 'b' if a else '')
    df['Category'] = df['Category'] + df['Has_Lift'].apply(lambda a: 'Lf' if a else '')
    df['Category'] = df['Category'] + df['Floor'].apply(lambda a: 'Bj' if a == 'bj' else '')
    df['Category'] = df['Category'] + df['Status'].apply(lambda a: '' if a == 'good' else a[0].upper())
    df['Size_Interval'] = df['Size_Interval'].astype('str')

    # Identificar pisos repetidos en la misma semana con diferente id
    cols_duplicated = ['Address', 'Size', 'Floor', 'Category', 'Subtitle', 'Title']
    df = df.sort_values(['Price'])
    df['Duplicated_hide'] = ~df[cols_duplicated].duplicated(keep='first')
    df['Duplicated'] = df[cols_duplicated].duplicated(keep=False)

    # Agrupar por vecindario, y añadir al data
    cols = ['Neighborhood']
    df_gp = df.loc[(df['Status'] == 'good') & (df['Duplicated_hide'])][cols + ['Price_m2', 'Price']] \
        .groupby(cols) \
        .agg(Price_m2_Neighb=('Price_m2', 'median'), Price_Neighb=('Price', 'median'), Neighb_count=('Price', 'count')) \
        .reset_index()
    df = df.merge(df_gp, on='Neighborhood', how='left')
    df['Price_m2_Neighb_Diff_pc'] = df[['Price_m2', 'Price_m2_Neighb']] \
        .apply(lambda p: round(100 * (p['Price_m2'] - p['Price_m2_Neighb']) / p['Price_m2_Neighb'], 1), axis=1)

    # Agrupar por vecindario y categoria, y añadir al data
    cols = ['Neighborhood', 'Category']
    df_gp = df.loc[(df['Status'] == 'good') & (df['Duplicated_hide'])][cols + ['Price_m2', 'Price']]\
        .groupby(cols) \
        .agg(Price_m2_Ngh_Ctg=('Price_m2', 'median'), Price_Ngh_Ctg=('Price', 'median')) \
        .reset_index()
    df = df.merge(df_gp, on=['Neighborhood', 'Category'], how='left')
    df['Price_m2_Ngh_Ctg_Diff_pc'] = df[['Price_m2', 'Price_m2_Ngh_Ctg']] \
        .apply(lambda p: round(100 * (p['Price_m2'] - p['Price_m2_Ngh_Ctg']) / p['Price_m2_Ngh_Ctg'], 1), axis=1)

    # Agrupar por districto, y añadir al data
    cols = ['District']
    df_gp = df.loc[(df['Status'] == 'good') & (df['Duplicated_hide'])][cols + ['Price_m2', 'Price']]\
        .groupby(cols) \
        .agg(Price_m2_Distr=('Price_m2', 'median'), Price_Distr=('Price', 'median'), Distr_count=('Price', 'count')) \
        .reset_index()
    df = df.merge(df_gp, on='District', how='left')
    df['Price_m2_Distr_Diff_pc'] = df[['Price_m2', 'Price_m2_Distr']] \
        .apply(lambda p: round(100 * (p['Price_m2'] - p['Price_m2_Distr']) / p['Price_m2_Distr'], 1), axis=1)

    # Unir data con los de la semana pasada
    df = df.merge(df_last[['Id', 'Price']].rename(columns={'Price': 'Price_Old'}), on='Id', how='left')
    if not df_last.empty:
        df_last.loc[~df_last['Id'].isin(df['Id']), 'Outputs'] = 1
        df_last['Outputs'].fillna(0, inplace=True)

    # Calculo de variaciones entre semanas
    df['Price_Diff'] = df[['Price', 'Price_Old']].apply(
        lambda a: 0 if np.isnan(a['Price']) or np.isnan(a['Price_Old']) else int(a['Price']) - int(a['Price_Old']),
        axis=1).fillna(0)
    df['Price_Diff_pc'] = df[['Price_Old', 'Price_Diff']] \
        .apply(lambda p: round(100 * p['Price_Diff'] / p['Price_Old'], 1) if p['Price_Diff'] != 0 else 0, axis=1)

    df[['Inputs', 'Keeps']] = df[['Price', 'Price_Old']].apply(
        lambda a: pd.Series({'I': not np.isnan(a['Price']) and np.isnan(a['Price_Old']),
                             'K': not np.isnan(a['Price']) and not np.isnan(a['Price_Old'])}), axis=1) * 1

    df[['Price_Up', 'Price_Down', 'Price_Keep']] = df['Price_Diff'].apply(
        lambda a: pd.Series({'Price_Up': a > 0, 'Price_Down': a < 0, 'Price_Keep': a == 0})) * 1

    # df_ds['Year'] = int(fecha.year)
    # df_ds['Month'] = 'M{:0>2}'.format(fecha.month)
    # df_ds['Week'] = 'W{:0>2}'.format(fecha.isocalendar()[1])

    # Añadimos el data de la semana anterior con el dato de las salidas
    if not df_last.empty:
        cols_last = sorted(df_last.columns)
        df_last[cols_last].fillna(0).to_json(path_etl + files[idx - 1], orient='records', lines=True)
        data = data.append(df_last[cols_last]).drop_duplicates()

    # Guardar el último data para comparar
    df_last = df.copy()

    if idx == len(files):
        # Añadimos el data de la ultima semana aun a falta del dato de las salidas
        cols = sorted(df.columns)
        cols_data = sorted(data.columns)
        data = data.append(df[cols]).drop_duplicates()

        # Guardar el último para el informe
        data[cols_data].to_csv(path_etl + 'idealista_oferta_casas_all.csv', index=False, float_format='%.2f', sep='|')

        path = r'C:\Users\aasensio\Google Drive (atanorcapital@gmail.com)\Idealista\idealista_api_last.xlsx'
        df['Url'] = df['Url'].apply(lambda h: '=HYPERLINK("%s", "%s")' % (h, h))
        df.to_excel(excel_writer=path, index=False)
