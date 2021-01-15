import sys
import pandas as pd
from os import listdir

print(sys.version)
print('Pandas version:', pd.__version__)

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_colwidth', 40)
pd.set_option('display.width', 200)


# FUNCIONES
def preparar_data_ine(df):
    df = df.loc[df['Nombre'].str.contains('Madrid')]
    df = df.join(df['MetaData'].apply(lambda row: pd.Series({y['Variable']['Nombre']: y['Nombre'] for y in row})))
    df = df.drop(['Nombre', 'COD', 'MetaData'], axis=1)
    df = df.explode('Data')
    df = pd.concat([df.drop(['Data'], axis=1), df['Data'].apply(pd.Series)], axis=1)
    return df.fillna('')


def data_ine_hipotecas_constituidas_api_13896(data_tmp):
    data_ine_hipotecas_api_13896 = pd.read_json(path_ine_hipotecas_constituidas_api_13896)
    data_ine_hip = preparar_data_ine(data_ine_hipotecas_api_13896)
    data_ine_hip = data_ine_hip.loc[data_ine_hip['Naturaleza de la finca'] == 'Viviendas']

    # Preparar data
    df = data_ine_hip[['T3_Unidad', 'T3_Escala', 'Naturaleza de la finca', 'Concepto financiero', 'T3_Periodo',
                       'Anyo', 'Valor']].drop_duplicates().reset_index(drop=True)
    df = df.replace(to_replace='Número', value='Numero', regex=True)
    df = df.rename(columns={'Concepto financiero': 'Segmento', 'T3_Unidad': 'Unidad', 'T3_Periodo': 'Periodo',
                            'Naturaleza de la finca': 'Naturaleza', 'T3_Escala': 'Escala'})
    df['Titulo'] = 'Hipotecas'
    df['Etiqueta'] = ''
    df['Escala'] = df['Escala'].apply(lambda y: y if y == 'Miles' else 'Unidad')
    df = df[['Titulo', 'Segmento', 'Etiqueta', 'Periodo', 'Anyo', 'Valor', 'Unidad', 'Escala']]

    # pivot Columns
    df2 = df.pivot(index=['Periodo', 'Anyo'], columns='Segmento', values='Valor').reset_index(level=[0, 1])
    df2['Titulo'] = 'Hipotecas'
    df2['Segmento'] = 'Pivot Columns'
    df2['Etiqueta'] = ''
    df2['Importe de hipotecas'] = df2['Importe de hipotecas'].astype('int64') * 1000
    df2['Numero de hipotecas'] = df2['Numero de hipotecas'].astype(int)
    df2['Importe Medio'] = df2['Importe de hipotecas'] / df2['Numero de hipotecas']
    df2['Importe Medio'] = df2['Importe Medio'].astype(int)
    df2['Valor'] = 0

    data_tmp = data_tmp.append(df, ignore_index=True)
    data_tmp = data_tmp.append(df2, ignore_index=True)
    return data_tmp


def data_ine_demografia_api(data_tmp):
    data_dem_pbl = pd.read_json(path_ine_demografia_api_2881)
    data_dem_pbl = preparar_data_ine(data_dem_pbl)

    df = data_dem_pbl.loc[data_dem_pbl['Municipios'] == 'Madrid']
    df = df.rename(columns={'Tamaño de los municipios': 'Titulo', 'T3_Unidad': 'Unidad', 'T3_Periodo': 'Periodo',
                            'Estado de la vivienda': 'Estado', 'Régimen de la vivienda': 'Regimen'})
    df = df[['Titulo', 'Sexo', 'Periodo', 'Anyo', 'Valor', 'Tipo de dato', 'Unidad']].drop_duplicates()
    df['Segmento'] = df['Sexo'].apply(lambda y: y if y == 'Total' else 'Genero')
    df['Etiqueta'] = df['Sexo'].apply(lambda y: '' if y == 'Total' else y)
    df['Escala'] = 'Unidad'

    df = df[['Titulo', 'Segmento', 'Etiqueta', 'Periodo', 'Anyo', 'Valor', 'Unidad', 'Escala']]
    data_tmp = data_tmp.append(df, ignore_index=True)
    return data_tmp


def data_compra_venta(data_tmp):
    data_cmp_vnt = pd.read_json(path_ine_compra_venta_api_6150)
    data_cmp_vnt = preparar_data_ine(data_cmp_vnt)

    df = data_cmp_vnt.rename(columns={'Título de adquisición': 'Titulo', 'T3_Unidad': 'Unidad', 'T3_Periodo': 'Periodo',
                                      'Estado de la vivienda': 'Estado', 'Régimen de la vivienda': 'Regimen'})
    df = df[['Titulo', 'Estado', 'Regimen', 'Periodo', 'Anyo', 'Valor', 'Tipo de dato', 'Unidad']].drop_duplicates()
    df['Segmento'] = df['Estado'].apply(lambda y: 'Total' if y == 'General'
                                        else 'Estado de la vivienda' if y != '' else 'Regimen de la vivienda')
    df['Etiqueta'] = df[['Estado', 'Regimen']].apply(lambda y: y['Estado'] if y['Regimen'] == ''
                                                     else y['Regimen'], axis=1)
    df['Escala'] = df['Tipo de dato'].apply(lambda y: 'Unidad' if y == 'Número' else '')

    df = df[['Titulo', 'Segmento', 'Etiqueta', 'Periodo', 'Anyo', 'Valor', 'Unidad', 'Escala']]
    data_tmp = data_tmp.append(df, ignore_index=True)
    return data_tmp


def data_demanda_google(data_tmp, campo):
    print(f'\n{campo}')
    cols = ['Titulo', 'Segmento', 'Etiqueta', 'Periodo', 'Anyo', 'Valor', 'Unidad', 'Escala']
    try:
        data_demanda = pd.read_csv(path_madrid + f'demanda {campo}.csv')[cols]
        data_demanda['Valor'] = data_demanda['Valor'].astype(float).astype(int)
        data_demanda['Anyo'] = data_demanda['Anyo'].astype(int)
    except:
        data_demanda = pd.DataFrame(columns=cols)

    onlyfiles = sorted([f for f in listdir(path_madrid) if f'Keyword Forecasts {campo}' in f])[-1]
    fecha: list = onlyfiles.split(f'{campo} ')[-1].split('.')[0].split('-')

    df = pd.read_csv(path_madrid + onlyfiles, encoding="utf-16", sep='\t')[0:1]
    df = df[['Estimated Impressions']]
    df['Titulo'] = 'Demanda %s' % campo.capitalize()
    df['Segmento'] = 'Google'
    df['Etiqueta'] = 'Busquedas'
    df['Periodo'] = 'M' + str(fecha[1])
    df['Anyo'] = int(fecha[0])
    df['Unidad'] = 'Busquedas'
    df['Escala'] = 'Unidad'
    df['Valor'] = df['Estimated Impressions'].str.replace(',', '.').astype(float).astype(int)
    df = df[['Titulo', 'Segmento', 'Etiqueta', 'Periodo', 'Anyo', 'Valor', 'Unidad', 'Escala']]

    data_demanda = data_demanda.append(df, ignore_index=True).drop_duplicates()
    data_demanda.to_csv(path_madrid + f'demanda {campo}.csv', index=False)
    data_tmp = data_tmp.append(data_demanda, ignore_index=True)
    return data_tmp


# PATH
path_madrid = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/Madrid/'
path_ine_hipotecas_constituidas_api_13896 = 'http://servicios.ine.es/wstempus/js/es/DATOS_TABLA/13896?tip=AM'
path_ine_demografia_api_2881 = 'http://servicios.ine.es/wstempus/js/es/DATOS_TABLA/2881?tip=AM'
# path_ine_demografia_api_36726 = 'http://servicios.ine.es/wstempus/js/es/DATOS_TABLA/36726?tip=AM'
# https://datos.gob.es/es/catalogo/ea0010587-indicadores-demograficos-anual-municipios-identificador-api-30934
# ine_demografia 'https://datos.gob.es/es/catalogo/ea0010587-poblacion-por-provincias-y-sexo-anual-cifras-oficiales-de-poblacion-de-los-municipios-espanoles-revision-del-padron-municipal-identificador-api-28521'
path_ine_compra_venta_api_6150 = 'http://servicios.ine.es/wstempus/js/es/DATOS_TABLA/6150?tip=AM'

# DATA
data = pd.DataFrame()

# DEMANDA GOOGLE
for x in ['pisos baratos', 'hipotecas']:
    data = data_demanda_google(data, x)

# DEMOGRAFIA
data = data_ine_demografia_api(data)

# COMPRA VENTA
data = data_compra_venta(data)

# HIPOTECAS CONSTITUIDAS
data = data_ine_hipotecas_constituidas_api_13896(data)

# ALL DATA
data['Valor'] = data['Valor'].astype(float).astype('int64')
data['Anyo'] = data['Anyo'].astype(int)
for x in ['Importe Medio', 'Numero de hipotecas', 'Importe de hipotecas']:
    data[x] = data[x].fillna(0).astype('int64')
data[x] = data[x].fillna('')

print(data.loc[((data['Periodo'] == 'M10') | (data['Periodo'] == 'A') | (data['Periodo'] == 'M01')) & (data['Anyo'] >= 2020)])
data.to_csv(path_madrid + 'data madrid all.csv', index=False, float_format='%.0f')

