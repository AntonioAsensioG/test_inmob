# -*- coding: utf-8 -*-

import re
import email
import imaplib
import smtplib
import inspect
import datetime
import numpy
import pandas as pd
from bs4 import BeautifulSoup
from email.mime.text import MIMEText

# CONFIG LIBRERIAS Y MODULOS
pd.set_option('display.max_colwidth', 30)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)


class GetHouseByGmail:

    def __init__(self):
        self.version = '1.0.0'

    @staticmethod
    def corregir_direccion(direccion):

        if len(direccion) != 4 :
            print(direccion)
        try:
            portal_ok = False
            # Caso con solo un campo
            if len(direccion) == 1:
                direccion.insert(0, None)
                direccion.insert(1, 's/n')
                direccion.append(direccion[2])
                return direccion

            # Caso: tres campos, falta el portal
            if direccion[1] != 's/n' and len(direccion) == 3:
                if direccion[1].strip().isnumeric():
                    portal_ok = True
                else:
                    direccion.insert(1, 's/n')

            # Caso: tres campos, portal ok
            if portal_ok:
                direccion.append(direccion[2])

            # Caso: 2 campos, la zona y7o ciudad, sin piso ni calle
            if len(direccion) == 2 and 'Laguna de Duero' not in direccion[-1]:
                direccion.insert(0, None)
                direccion.insert(1, 's/n')

            if len(direccion) == 2 and 'Laguna de Duero' in direccion[-1]:
                direccion.insert(1, 's/n')
                direccion.append(direccion[2])

            if len(direccion) == 5 and ' Madrid' in direccion:
                direccion.remove(' Madrid')

            return direccion
        except Exception as error_cd:
            print("Error", direccion)
            print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_cd))
            raise ValueError(error_cd)

    def normalizar_houses(self, df):
        try:
            # Id
            df['Id'] = df['Url'].apply(lambda i: int(i.split('/')[4]))

            # Hiperlink
            df['Link'] = df['Url'].apply(lambda h: '=HYPERLINK("%s", "%s")' %('/'.join(h.split('[')[0].split('/')[0:5]),
                                                                             '/'.join(h.split('[')[0].split('/')[0:5])))

            # Precio
            df[['Precio', 'Precio_old']] = df['Precio'].str.split(pat='%', expand=True).reindex([0, 1], axis=1)
            df['Precio'] = df['Precio'].apply(lambda y: int(re.sub('[.]', '', y.split('€')[0])))
            df['Precio_old'] = df['Precio_old'].apply(lambda z: int(re.sub('[.€]', '', z)) if type(z) is str else None)

            # Info Piso
            df['Vivienda'] = df['Vivienda'].apply(lambda v: re.sub(r'\n', ' ', v).strip())
            df['Tamaño'] = df['Vivienda'].apply(lambda v: re.findall(r'(\d+)(?: m)', v)[0:1])
            df['Habitaciones'] = df['Vivienda'].apply(lambda u: re.findall(r'(\d+)(?: hab)', u)[0:1])
            df['Planta'] = df['Vivienda'].apply(lambda z: re.findall(r'(?:hab\. )(.+?)$', z)[0:1])

            # Info Descripcion
            df['Descripcion'] = df['Descripcion'].apply(lambda d: re.sub(r'\n', ' ', d) if type(d) is str else None)
            df['Descripcion'] = df['Descripcion'].apply(lambda e: e.strip() if type(e) is str else None)

            # Info Direccion
            df['Tipo'] = df['Direccion'].apply(lambda a: re.findall(r'(^.+?)(?: en)', a)[0:1])
            df['tmp'] = df['Direccion'].apply(lambda a: re.sub(r'^.+? en ', '', a))
            df['tmp'] = df['tmp'].str.split(',', expand=False)
            df['tmp'] = df['tmp'].apply(lambda a: a if len(a) == 4 else self.corregir_direccion(a))
            df[['Calle', 'Portal', 'Zona', 'Ciudad']] = pd.DataFrame(df['tmp'].tolist())
            # eliminar espacio vacios antes y despues, poner minuscular
            for w in ['Calle', 'Portal', 'Zona', 'Ciudad']:
                df[w] = df[w].str.strip().str.lower()
            df = df.drop('tmp', axis=1)

            # Eliminar valores en formato lista
            for x in ['Tamaño', 'Habitaciones', 'Planta', 'Tipo']:
                df[x] = df[x].apply(lambda y: y[0].strip() if y else None)

            # String numerico to integer
            for x in ['Tamaño', 'Habitaciones']:
                df[x] = df[x].apply(lambda z: int(z) if str(z).isdigit() else None)

            return df
        except Exception as error_nh:
            print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_nh))
            raise ValueError(error_nh)

    def get_body_email(self, tmsg):
        try:
            date = 0
            df_houses = pd.DataFrame()
            body = ""
            fd = '%Y-%m-%d %H:%M:%S'    # format of date

            # Dar formato a la fecha
            for f in ['%a, %d %b %Y %H:%M:%S +%f',
                      '%a, %d %b %Y %H:%M:%S +%f (CEST)',
                      '%a, %d %b %Y %H:%M:%S -%f (CEST)']:
                try:
                    date = str(datetime.datetime.strptime(tmsg['Date'], f).strftime(fd))
                    break
                except:
                    continue

            if tmsg.is_multipart():
                for part in tmsg.walk():
                    ctype = part.get_content_type()
                    cdispo = str(part.get('Content-Disposition'))

                    if ctype == 'text/plain' and 'attachment' not in cdispo:
                        # body = part.get_payload(decode=True)
                        # body = body.decode('utf-8', 'ignore')
                        # body = body.replace("\\r\\n", "\n")
                        print('ctype == "text/plain"')
                        return pd.DataFrame()

                    elif ctype == 'text/html' and 'attachment' not in cdispo:
                        body = part.get_payload(decode=True)
                        body = body.decode('utf-8', 'ignore')

                        # Contenido a html con bs4
                        soup = BeautifulSoup(body, 'html.parser')
                        print(tmsg['Subject'])

                        if "RESUMEN DIARIO DE NUEVOS" in tmsg['Subject'].upper():
                            tables_00 = soup.find_all('table')
                            for i, x in enumerate(tables_00):
                                tables_01 = x.find_all('tbody')
                                for j, y in enumerate(tables_01):
                                    tables_02 = y.find_all('table')
                                    for k, z in enumerate(tables_02):
                                        tables_03 = z.find_all('tbody')
                                        for h, v in enumerate(tables_03):
                                            tables_04 = v.find_all('table')
                                            for m, w in enumerate(tables_04):
                                                tables_05 = w.find_all('tbody')
                                                for n, q in enumerate(tables_05):
                                                    tables_06 = q.find_all('table')
                                                    if tables_06:
                                                        try:
                                                            url = tables_06[0].find('a', href=True)
                                                            df_result = pd.read_html(str(tables_06[0]))[0].T
                                                            df_result = df_result.drop(df_result.shape[1]-1, axis=1)
                                                            df_result['Url'] = url['href']
                                                            df_result = df_result.rename(columns={0: 'Direccion',
                                                                                                  1: 'Precio',
                                                                                                  2: 'Vivienda',
                                                                                                  3: 'Descripcion'})
                                                            df_houses = df_houses.append(df_result, ignore_index=True, sort=True)
                                                        except:
                                                            pass

                            df_houses['Fecha'] = date
                            print(df_houses)
                            return self.normalizar_houses(df_houses)

                        elif "Nuevo_piso_en_tu_b=C3=BAsqueda" in tmsg['Subject'] or \
                            "Nuevo_=C3=A1tico_en_tu_b=C3=BAsqueda" in tmsg['Subject'] or \
                            "Nuevo_duplex_en_tu_b=C3=BAsqueda" in tmsg['Subject']:
                            print('\t\tEntra en Nueva Vivienda')
                            d = {}

                            tables_00 = soup.find_all("td", attrs={'align': "left",
                                    'style': "font-size:0px;padding:0 0 10px;word-break:break-word;"})
                            d['Direccion'] = tables_00[0].find('a')['title']
                            d['Url'] = tables_00[0].find('a')['href']

                            tables_00 = soup.find_all("td", attrs={'align': "left",
                                    'style':"font-size:0px;padding:0 0 8px;word-break:break-word;"})
                            d['Descripcion'] = tables_00[-1].text

                            tables_00 = soup.find_all("table", attrs={'cellpadding':'0', 'cellspacing':'0', 'width':'100%', 'border':'0',
                                'style':"cellspacing:0;color:#000000;font-family:Arial;font-size:13px;line-height:22px;table-layout:auto;width:100%;"})
                            d['Vivienda'] = tables_00[0].find_all('td')[1].text
                            d['Precio'] = tables_00[0].find_all('td')[0].text

                            return self.normalizar_houses(pd.DataFrame([d]))

                        elif "2Nuevo_piso_en_tu_b=C3=BAsqueda" in tmsg['Subject'] or \
                           "Nuevo_chalet" in tmsg['Subject']:
                            print('\t\tEntra en Nueva Vivienda')
                            tables_00 = soup.find_all('table')
                            for a in tables_00:
                                try:
                                    url = a.find('a', href=True)
                                    df_result = pd.read_html(str(a))[0]
                                    print(df_result)
                                    df_houses = df_result.T.copy()
                                    df_houses = df_houses.rename(columns={10: 'Direccion',
                                                                          16: 'Precio',
                                                                          17: 'Vivienda',
                                                                          18: 'Descripcion'})
                                    df_houses = df_houses[['Direccion', 'Precio', 'Vivienda', 'Descripcion']]
                                    df_houses['Url'] = url['href']
                                    print(df_houses)
                                    break
                                except Exception as error:
                                    # print(error)
                                    pass
                            df_houses['Fecha'] = date
                            return self.normalizar_houses(df_houses)

                        elif "Bajada_de_precio" in tmsg['Subject']:
                            try:
                                dict_house = {}
                                divs = soup.find_all('div',
                                    attrs={'style': "background:#ffffff;background-color:#ffffff;Margin:0px auto;max-width:552px;"})

                                dict_house['Url'] = divs[1].a['href'].split('[')[0]
                                dict_house['Direccion'] = divs[1].a['title']

                                precio_old = divs[2].find_all('td', attrs={'style': "color:#d80000;font-size:16px; font-weight: 400;"})
                                dict_house['Precio_old'] = precio_old[0].text

                                precio = divs[2].find_all('td', attrs={'style': "color: #333; font-size: 20px; font-weight: 700; padding-bottom: 8px;"})
                                dict_house['Precio'] = precio_old[0].text + ' ' + precio[0].text

                                vivienda = divs[2].find_all('td', attrs={'style': "color: #333; font-size: 14px; padding-bottom: 8px;"})
                                dict_house['Vivienda'] = vivienda[0].text

                                dict_house['Descripcion'] = divs[3].text

                            except Exception as error:
                                print(error)
                                raise
                            dict_house['Fecha'] = date
                            return self.normalizar_houses(pd.DataFrame.from_records([dict_house]))

                        else:
                            print('else')
                            return pd.DataFrame()

                # else:
                #    body = str(tmsg.get_payload(decode=True))
            return pd.DataFrame()

        except Exception as error_gbe:
            print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_gbe))
            raise ValueError(error_gbe)

    def get_emails(self, gmailsmtpsvr, gmailusr, gmailpwd, subj_email, limit=1, move_email=False):
        """
        Gmail necesita un ssl socket (encriptado) por lo que utilizamos la subclase IMAP4_SSL
        Parámetros:  El puerto por defecto IMAP4_SSL_PORT es el 993 (en el caso de gmail), host='', port=IMAP4_SSL_PORT,
        keyfile=None, certfile=None, ssl_context=None y el resto de parametros no son necesarios en este caso.
        Únicamente es necesario pasar como parámetro el servidor stmp de gmail
        :param move_email:
        :return:
        :param limit:
        :param subj_email:
        :param gmailsmtpsvr:
        :param gmailusr:
        :param gmailpwd:
        :return:
        """
        try:
            content_emails = pd.DataFrame()
            for i in range(0, limit):
                imap = imaplib.IMAP4_SSL(gmailsmtpsvr)
                imap.login(gmailusr, gmailpwd)
                imap.select("inbox")
                # print('Bandejas', mail.list())

                imap.literal = subj_email.encode("utf-8")
                result, data = imap.search("utf-8", 'SUBJECT')

                lstids = data[0].split()                       # coge lista de ids encontrados y los separa para bucle
                print('Num of emails from %s:' % subj_email, len(lstids))
                if len(lstids) == 0:
                    break

                # recorremos lista de mayor a menor (mas recientes primero)
                for item in lstids:
                    typ, data = imap.fetch(item, '(RFC822)')        # el parámetro id esperado es tipo cadena

                    for contenido in data:
                        if isinstance(contenido, tuple):                     # comprueba que 'contenido' sea una tupla
                            msg = email.message_from_string(contenido[1].decode())  # recuperamos información del email:
                            print('\tSubject:', msg['Subject'], ', Date:', msg['Date'])          # Date, from, to

                            # Extraer Cuerpo del email
                            content_email = self.get_body_email(msg)
                            if content_email.empty:
                                print("\tContenidos recuperados 0, vacio")
                            else:
                                print("\tContenidos recuperados %d\n" % content_email.shape[0])
                                content_emails = content_emails.append(content_email, sort=True)

                                if move_email:
                                    imap.copy(item, 'Idealista')
                                    imap.store(item, '+FLAGS', r'\Deleted')
                                    imap.store(item, '-FLAGS', r'Inbox')
                                    imap.expunge()

            print("\tContenidos totales recuperados %d" % len(content_emails))
            return content_emails

        except Exception as error_ge:
            print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_ge))
            raise ValueError(error_ge)


def send_email(from_addr, username, password):

    # connect with Google's servers
    smtp_ssl_host = 'smtp.gmail.com'
    smtp_ssl_port = 465
    # use username or email to log in

    to_addrs = ['tonytroker@gmail.com']

    # the email lib has a lot of templates for different message formats, on our case we will use MIMEText
    # to send only text
    message = MIMEText('Hello World')
    message['subject'] = 'Hello'
    message['from'] = from_addr
    message['to'] = ', '.join(to_addrs)

    # we'll connect using SSL
    server = smtplib.SMTP_SSL(smtp_ssl_host, smtp_ssl_port)
    # to interact with the server, first we log in
    # and then we send the message
    server.login(username, password)
    server.sendmail(from_addr, to_addrs, message.as_string())
    server.quit()


def save_houses(data, save_files = True):
    path_xls = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/idealista_%s.xlsx' % 'news'
    path_xls = 'C:/Users/aasensio/Google Drive (atanorcapital@gmail.com)/Idealista/viviendas_idealista.xlsx'
    path_json = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/idealista_%s.json' % 'news'
    cols_dupli = ['Id', 'Tipo', 'Tamaño', 'Habitaciones', 'Planta', 'Calle', 'Portal', 'Zona', 'Ciudad', 'Precio',
                  'Precio_old']
    cols_to_save = cols_dupli + ['Descripcion', 'Fecha', 'Link']
    try:
        print('\nNum of houses nuevos:', data.shape[0])
        data = data.drop_duplicates()
        print('Num of houses nuevos no duplicados:', data.shape[0])

        try:
            df_json = pd.read_json(path_json, orient='records', lines=True, encoding="utf8")
        except:
            df_json = pd.DataFrame()
        print('Num houses ya existentes', df_json.shape[0])

        # Juntar todos los registros
        data = data.append(df_json, ignore_index=True, sort=True)

        # Calcular la longuitud de la descripcion
        data['Len_descp'] = data['Descripcion'].str.len()

        # Calcular veces que aparce un id
        data['Id_count'] = data['Id'].map(data['Id'].value_counts())

        # Eliminar duplicados, no considerar Fecha y Descripcion, ordenamos por Id y Descrip para mantener la mas larga
        data = data.sort_values(['Id', 'Len_descp'], ascending=[False, False])
        data = data.drop_duplicates(cols_dupli)
        print('Num houses totales (elimina duplicados sin descripcion)', data.shape[0])

        if save_files:
            data['Precio/m2'] = data[['Precio', 'Tamaño']].apply(lambda x: \
                                    round(int(x['Precio'])/int(x['Tamaño']), 0) if x['Tamaño'] > 0 else '', axis=1)
            data[cols_to_save+ ['Precio/m2', 'Id_count']].to_excel(path_xls, index=False)
            data.to_json(path_json, orient='records', lines=True)

    except Exception as error_sv:
        print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_sv))
        raise ValueError(error_sv)


if __name__ == '__main__':
    email_account = "atanorcapital@gmail.com"
    user = email_account
    user_password = "Atanor2020"
    cols_num = ['Precio', 'Precio_old', 'Tamaño']
    subjects = ['Nuevo piso en tu búsqueda', 'Nuevo ático', 'Nuevo duplex', '"Resumen diario de nuevos anuncios"',
                'Bajada de precio']

    resultados = pd.DataFrame()
    for subject in subjects * 5:

        resultado = GetHouseByGmail().get_emails("smtp.gmail.com", email_account, user_password, subject, 1, True)
        resultados = resultados.append(resultado, ignore_index=True, sort=True)

    save_houses(resultados, save_files=True)
    # send_email(email, user, password)
