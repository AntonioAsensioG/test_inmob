#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import email
import imaplib
import smtplib
import inspect
import datetime
# import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
from email.header import decode_header

# CONFIG LIBRERIAS Y MODULOS
pd.set_option('display.max_colwidth', 30)
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)


class GetHouseByGmail:

    def __init__(self):
        self.version = '1.0.0'

    @staticmethod
    def get_body_email(tmsg):
        try:
            df_houses = pd.DataFrame()

            # Dar formato a la fecha
            f = '%a, %d %b %Y %H:%M:%S +%f (UTC)'
            try:
                date = str(datetime.datetime.strptime(tmsg['Date'], f).strftime('%Y-%m-%d %H:%M:%S'))
            except:
                date = 0

            for part in tmsg.walk():
                body = part.get_payload(decode=True)
                body = body.decode('utf-8', 'ignore')

                # Contenido a html con bs4
                soup = BeautifulSoup(body, 'lxml')
                subj = decode_header(tmsg['Subject'])[0][0].decode().lower()
                if " en varios distritos de madrid" in subj or "¡visítalos sin salir de casa!" in subj:
                    divs_soup = soup\
                        .find_all('table', attrs={'align': "center", 'border': "0", 'cellpadding': "0",
                                                  'cellspacing': "0", 'role': "presentation",
                                                  'style': "background:#f5f5f5;background-color:#f5f5f5;width:100%;"})

                    for i, x in enumerate(divs_soup):
                        texto = divs_soup[i].getText()
                        if 'Expertos en la Zona' in texto:
                            continue

                        dict_house = {'Precio': int(''.join(next(iter(re.findall(r'(\d+)(?:\.?)(\d+)(?: €)', texto)), '0'))),
                                      'Habitaciones': int(next(iter(re.findall(r'(\d+)(?: habs?)', texto)), -1)),
                                      'Tamaño': int(next(iter(re.findall(r'(\d+)(?: m)', texto)), -1)),
                                      'Baños': int(next(iter(re.findall(r'(\d+)(?: baños?)', texto)), -1)),
                                      'Zona': next(iter(re.findall(r'Piso .+?$', texto)), '').replace('Piso en ', ''),
                                      'Url': divs_soup[i].a['href'].split('[')[0]}
                        df_houses = df_houses.append(pd.DataFrame([dict_house]), ignore_index=True, sort=True)

                    if df_houses.empty:
                        continue

                    df_houses['Fecha'] = date
                    df_houses['Id'] = df_houses['Url'].apply(lambda z: int(z.split('/')[8]))
                    df_houses['Tipo'] = df_houses['Zona'].apply(lambda a: next(iter(re.findall(r'(^.+?)(?: en)', a)), ''))
                    df_houses['Link'] = df_houses['Url'].apply(lambda h: '=HYPERLINK("%s", "%s")' % (h, h))
                    # df[['Calle', 'Portal', 'Zona', 'Ciudad']] = pd.DataFrame(df['tmp'].tolist())

                else:
                    print('else')

            return df_houses

        except Exception as error_gbe:
            print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_gbe))
            raise ValueError(error_gbe)

    def get_emails(self, gmailsmtpsvr, gmailusr, gmailpwd, from_email, limit=1, move_email=False):
        """
        Gmail necesita un ssl socket (encriptado) por lo que utilizamos la subclase IMAP4_SSL
        Parámetros:  El puerto por defecto IMAP4_SSL_PORT es el 993 (en el caso de gmail), host='', port=IMAP4_SSL_PORT,
        keyfile=None, certfile=None, ssl_context=None y el resto de parametros no son necesarios en este caso.
        Únicamente es necesario pasar como parámetro el servidor stmp de gmail
        :param move_email:
        :return:
        :param limit:
        :param from_email:
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

                imap.literal = from_email.encode("utf-8")
                ok, ids = imap.search("utf-8", 'FROM')

                lstids = ids[0].split()  # coge lista de ids encontrados y los separa para bucle
                print('Num of emails from %s:' % from_email, len(lstids))
                if len(lstids) == 0:
                    break

                # recorremos lista de mayor a menor (mas recientes primero)
                for item in lstids[0:1]:
                    ok, data = imap.fetch(item, '(RFC822)')  # el parámetro id esperado es tipo cadena
                    for contenido in data:
                        if isinstance(contenido, tuple):  # comprueba que 'contenido' sea una tupla
                            #print(contenido)
                            msg = email.message_from_string(contenido[1].decode())  # recuperamos información del email:
                            print(type(msg))
                            print('\nkeys',msg.keys())
                            print('\nitems',msg.items())
                            print('\nget_content_type', msg.get_content_type())
                            print('\nget_content_maintype', msg.get_content_maintype())
                            print('\nContent-Type', msg['Content-Type'])

                            try:
                                print(decode_header(msg['Subject'])[0][0].decode())
                            except:
                                print('Exception', msg['Subject'])

                            print('\t', 'Date:', msg['Date'], 'From:', msg['From'])  # to

                            if from_email != re.split('[<>]', msg['From'])[1]:
                                print('From no coincide')
                                break

                            # Extraer Cuerpo del email
                            content_email = self.get_body_email(msg)
                            if content_email.empty:
                                print("\tContenidos recuperados 0, vacio")
                            else:
                                print("\tContenidos recuperados %d\n" % content_email.shape[0])
                                content_emails = content_emails.append(content_email, sort=True)

                                if move_email:
                                    imap.copy(item, 'Fotocasa')
                                    imap.store(item, '+FLAGS', r'\Deleted')
                                    imap.store(item, '-FLAGS', r'Inbox')
                                    imap.expunge()
                    exit()  # TODO
            print("\tContenidos totales recuperados %d" % len(content_emails))
            print(content_emails)
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


def save_houses(data: pd.DataFrame, save_files: object = True):
    path_xls = 'C:/Users/aasensio/Google Drive (atanorcapital@gmail.com)/Fotocasa/viviendas_fotocasa.xlsx'
    path_json = 'C:/Users/aasensio/PycharmProjects/test_inmob/Data/fotocasa.json'
    cols_dupli = ['Id', 'Tipo', 'Tamaño', 'Habitaciones', 'Zona', 'Baños', 'Precio']

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

        # Eliminar duplicados, no considerar Fecha y Descripcion, ordenamos por Id y Descrip para mantener la mas larga
        data = data.sort_values(['Id'], ascending=[False])
        data = data.drop_duplicates(cols_dupli)
        print('Num houses totales (elimina duplicados sin descripcion)', data.shape[0])

        # Guardar resultados
        if save_files:
            data['Precio/m2'] = data[['Precio', 'Tamaño']]\
                .apply(lambda x: round(x['Precio'] / x['Tamaño'], 0) if x['Tamaño'] > 0 else '', axis=1)
            data.to_json(path_json, orient='records', lines=True)
            data.to_excel(path_xls, index=False)

    except Exception as error_sv:
        print("Error in %s: %s" % (str(inspect.currentframe().f_code.co_name), error_sv))
        raise ValueError(error_sv)


if __name__ == '__main__':

    # email_account = "asesorantonioasensiog@gmail.com"
    email_account = "atanorcapital@gmail.com"
    user = email_account
    # user_password = "Adrian13Marco18"
    user_password = "Atanor2020"
    cols_num = ['Precio', 'Precio_old', 'Tamaño']
    subjects = ['enviosfotocasa@fotocasa.es']

    resultados = pd.DataFrame()
    for subject in subjects * 4:
        resultado = GetHouseByGmail().get_emails("smtp.gmail.com", email_account, user_password, subject, 2, True)
        resultados = resultados.append(resultado, ignore_index=True, sort=True)

    if not resultados.empty:
        save_houses(resultados, save_files=True)
        # send_email(email, user, password)
