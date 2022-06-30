from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google.oauth2 import service_account
from psycopg2 import Error
import psycopg2
import logging
import requests
from bs4 import BeautifulSoup
import time

#read google cheets and parser RYB from cbr.ru
while True:
    time.sleep(1)
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    service_account_file = os.path.join(BASE_DIR, 'credentials.json')

    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)

    SAMPLE_SPREADSHEET_ID = '1_64X4gEW04yhZ8AVRROyM2sS99LlBTqpK54oloTU4DU'
    SAMPLE_RANGE_NAME = 'sheet1!A2:E'

    service = build('sheets', 'v4', credentials=credentials)

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()

    values = result.get('values')

    url = 'https://cbr.ru/'

    source = requests.get(url)
    main_text = source.text
    soup = BeautifulSoup(main_text, "html.parser")

    table = soup.find('main', {'class': 'home-content'})
    table2 = table.find('div', {'class': 'main-indicator_rate'})
    ryb = table2.find('div', {'class': 'col-md-2 col-xs-9 _right mono-num'})
    ryb = ryb.text
    ryb = ryb[:2]
#read google cheets
    for row in values:
        id = row[0]
        zak = row[1]
        dol = row[2]
        data = row[3]
        ryb2 = int(dol) * int(ryb)
        print(id, zak, dol, ryb2, data)
#connetc ty DB nad insert data
        try:

            connection = psycopg2.connect(user="user",
                                          password="pass",
                                          host="ip",
                                          port="port",
                                          database="name database")

            cursor = connection.cursor()
            insert_query = """ INSERT INTO googlesheet (
            id,
            zak,
            dol,
            pyb,
            data) 
            VALUES(%s, %s, %s, %s, %s)"""
            record_insert = (id, zak, dol, ryb2, data)
            cursor.execute(insert_query, record_insert)
            connection.commit()
            print("1 запись успешно вставлена")
            cursor.execute("SELECT * from googlesheet")
            record = cursor.fetchall()
            print("Результат", record)
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
            logging.critical('Ошибка при работе с PostgreSQL')
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")
                logging.info("Добавлена информация в БД")