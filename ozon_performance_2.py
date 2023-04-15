import requests
import json
from datetime import datetime, date, timedelta
import time
import os
import pandas as pd
import numpy as np
import glob
import zipfile
import psycopg2
from sqlalchemy import create_engine


class OzonPerformanceEcom2:
    def __init__(self, client_id: str,
                 client_secret: str,
                 account_id=None
                 ):

        self.client_id = client_id
        self.client_secret = client_secret
        self.account_id = account_id

        self.auth = self.get_token()

        self.get_stat_report = []
        self.uuid_report = []

        self.camp_lim = 8
        self.day_lim = 30

    def get_token(self):
        """
        Авторизация с получением токена
        """
        url = 'https://performance.ozon.ru/api/client/token'
        head = {"Content-Type": "application/json",
                "Accept": "application/json"
                }
        body = {"client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials"
                }
        response = requests.post(url, headers=head, data=json.dumps(body))
        if response.status_code == 200:
            print('Подключение успешно, токен получен')
            return response.json()
        else:
            print(response.text)
            return None

    def get_campaigns(self, active_only=False):
        """
        Получение кампаний
        """
        url = 'https://performance.ozon.ru:443/api/client/campaign'
        # url = 'https://performance.ozon.ru:443/api/client/campaign?state=CAMPAIGN_STATE_RUNNING'
        head = {"Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": self.auth['token_type'] + ' ' + self.auth['access_token']
                }
        response = requests.get(url, headers=head)
        if response.status_code == 200:

            campaigns = pd.DataFrame(response.json()['list'])

            if active_only is True:
                campaigns = campaigns[campaigns['state'] == 'CAMPAIGN_STATE_RUNNING']

            print(f"Найдено кампаний: {len(campaigns)}")
            return campaigns.id.values.tolist()
        else:
            print(response.text)
            return None

    # @staticmethod
    # def split_list(list_: list, lenght: int):
    #     """Разбивает список на подсписки заданной длины (последний подсписок - остаток)"""
    #
    #     if len(list_) >= lenght:
    #         data = []
    #         for i in range(0, len(list_), lenght):
    #             data.append(list(list_)[i:i + lenght])
    #     else:
    #         data = [list_]
    #
    #     return data
    #
    # @staticmethod
    # def split_time(date_from, date_to, n_days: int):
    #     """Разбивает интервал на подинтервалы заданной длины (последний подинтервал - остаток)"""
    #
    #     delta = (date_to - date_from).days
    #     if delta > n_days:
    #         tms = []
    #         for t in range(0, delta, n_days):
    #             df = date_from + timedelta(days=t)
    #             to = date_from + timedelta(days=t + n_days - 1)
    #             if to >= date_to:
    #                 dt = date_to
    #             else:
    #                 dt = to
    #             tms.append([df, dt])
    #     else:
    #         tms = [[date_from, date_to]]
    #
    #     return tms

    @staticmethod
    def split_data(campaigns: list[str], camp_lim: int):
        """
        Разбивает данные в соответствии с ограничениями Ozon
        """
        if len(campaigns) >= camp_lim:
            data = []
            for i in range(0, len(campaigns), camp_lim):
                data.append(campaigns[i:i + camp_lim])
        else:
            data = [campaigns]
        return data

    @staticmethod
    def split_time(date_from: str, date_to: str, day_lim: int):
        """
        Разбивает временной промежуток в соответствии с лимитом Ozon
        """
        delta = datetime.strptime(date_to, '%Y-%m-%d') - datetime.strptime(date_from, '%Y-%m-%d')
        if delta.days > day_lim:
            tms = []
            for t in range(0, delta.days, day_lim):
                dt_fr = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t)).date())
                if (datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t + day_lim - 1)).date() >= \
                        (datetime.strptime(date_to, '%Y-%m-%d')).date():
                    dt_to = str((datetime.strptime(date_to, '%Y-%m-%d')).date())
                else:
                    dt_to = str((datetime.strptime(date_from, '%Y-%m-%d') + timedelta(days=t + day_lim - 1)).date())
                tms.append([dt_fr, dt_to])
        else:
            tms = [[date_from, date_to]]

        return tms

    def get_statistics(self,
                       campaigns: list,
                       date_from,
                       date_to,
                       group_by='DATE',
                       n_attempts=5,
                       delay=10
                       ):
        """Формирует запрос статистики, возвращает UUID и формат"""

        url = 'https://performance.ozon.ru:443/api/client/statistics'

        head = {
            "Authorization": f"{self.auth['token_type']} {self.auth['access_token']}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        body = {
            "campaigns": campaigns,
            "dateFrom": str(date_from),
            "dateTo": str(date_to),
            "groupBy": group_by
        }

        response = requests.post(url, headers=head, data=json.dumps(body))

        if response.status_code == 200:
            print('Campaign statistics UUID - ok')
            if len(campaigns) == 1:
                return {'UUID': response.json()['UUID'], 'format': 'csv'}
            else:
                return {'UUID': response.json()['UUID'], 'format': 'zip'}
        elif response.status_code == 429:
            n = 0
            while n < n_attempts:
                time.sleep(delay)
                response = requests.post(url, headers=head, data=json.dumps(body))
                print(f"status code {response.status_code}")
                if response.status_code == 200:
                    print('Campaign statistics UUID - ok')
                    if len(campaigns) == 1:
                        return {'UUID': response.json()['UUID'], 'format': 'csv'}
                    else:
                        return {'UUID': response.json()['UUID'], 'format': 'zip'}
                else:
                    n += 1
            print(f'Request declined {n_attempts} times')
            self.get_stat_report.append(
                {'account_id': self.account_id, 'date_from': date_from, 'date_to': date_to, 'ids': campaigns, 'status': 'declined'})
            return None
        else:
            self.get_stat_report.append(
                {'account_id': self.account_id, 'date_from': date_from, 'date_to': date_to, 'ids': campaigns, 'status': 'declined'})
            print(f'Error statistics {response.status_code}')
            return None

    def status_report(self, uuid):
        """Возвращает статус отчета"""

        url = f"https://performance.ozon.ru:443/api/client/statistics/{uuid}"

        head = {"Authorization": f"{self.auth['token_type']} {self.auth['access_token']}",
                "Content-Type": "application/json",
                "Accept": "application/json"
                }

        response = requests.get(url, headers=head)

        if response.status_code == 200:
            return response.json()
        else:
            print(response.text)
            return None

    def download_report(self, uuid):
        """Загружает файл отчета"""

        url = f"https://performance.ozon.ru:443/api/client/statistics/report?UUID={uuid}"

        head = {"Authorization": f"{self.auth['token_type']} {self.auth['access_token']}"}
        response = requests.get(url, headers=head)
        if response.status_code == 200:
            return response
        else:
            print(response.text)
            return None

    def get_report(self, uuid, format_, path):
        """Проверяет статус и по готовности загружает файл отчета"""

        # auth = self.get_token()

        # if auth is not None:
        try:
            status = None
            n = 0
            while status != 'OK':
                time.sleep(10)
                status = self.status_report(uuid)['state']
                n += 1
                print(f"{uuid} {status} {n}")
                if n == 150:
                    break

            if n < 150:
                report = self.download_report(uuid)
            else:
                report = None

            if report is not None:
                if format_ == 'csv':
                    file = f"{path}/{uuid}.csv"
                    with open(file, 'wb') as f:
                        f.write(report.content)
                    print(f"Saved {file}")
                elif format_ == 'zip':
                    file = f"{path}/{uuid}.zip"
                    with open(file, 'wb') as f:
                        f.write(report.content)
                    with zipfile.ZipFile(file, 'r') as zf:
                        zf.extractall(path)
                    os.remove(file)
                else:
                    return None
                return 'OK'
            else:
                print("Download error")
                return None
        except Exception as e:
            print("Token expired or unknown error")
            print(e)
            return None

    def collect_statistics(self, date_from: str, date_to: str, active_only=True):

        campaigns = self.get_campaigns(active_only=active_only)

        if len(campaigns) > 0:

            data = self.split_data(campaigns=campaigns, camp_lim=self.camp_lim)
            time_ = self.split_time(date_from=date_from, date_to=date_to, day_lim=self.day_lim)

            st_camp = []

            try:
                for d in data:
                    for t in time_:
                        st_camp.append(self.get_statistics(campaigns=d, date_from=t[0], date_to=t[1]))

                return st_camp

            except KeyError:
                return []
        else:
            return []

    def save_statistics(self, st_camp: list[dict], path_: str):

        folder = path_ + f'{self.account_id}-{self.client_id}/'
        if not os.path.isdir(folder):
            os.mkdir(folder)

        if not os.path.isdir(folder + 'statistics'):
            os.mkdir(folder + 'statistics')

        for st in st_camp:
            if st is not None:
                self.get_report(uuid=st['UUID'], format_=st['format'], path=(folder + 'statistics'))

    @staticmethod
    def make_dataset(path):
        """Собирает датасет из загруженных данных"""

        columns = {'ID заказа': 'order_id',
                   'Номер заказа': 'order_number',
                   'Ozon ID': 'ozon_id',
                   'Ozon ID рекламируемого товара': 'ozon_id_ad_sku',
                   'Артикул': 'articul',
                   'Ставка, %': 'search_price_perc',
                   'Ставка, руб.': 'search_price_rur',
                   'Тип страницы': 'pagetype',
                   'Условие показа': 'viewtype',
                   'Показы': 'views',
                   'Клики': 'clicks',
                   'CTR (%)': 'ctr',
                   'Средняя ставка за 1000 показов (руб.)': 'cpm',
                   'Заказы модели': 'orders_model',
                   'Выручка с заказов модели (руб.)': 'revenue_model',
                   'Тип условия': 'request_type',
                   'Платформа': 'platfrom',
                   'Охват': 'audience',
                   'Баннер': 'banner',
                   'Средняя ставка (руб.)': 'avrg_bid',
                   'Расход за минусом бонусов (руб., с НДС)': 'exp_bonus',
                   'Дата': 'data',
                   'День': 'data',
                   'Наименование': 'name',
                   'Название товара': 'name',
                   'Количество': 'orders',
                   'Заказы': 'orders',
                   'Цена продажи': 'price',
                   'Цена товара (руб.)': 'price',
                   'Выручка (руб.)': 'revenue',
                   'Стоимость, руб.': 'revenue',
                   'Расход (руб., с НДС)': 'expense',
                   'Расход, руб.': 'expense',
                   'Unnamed: 1': 'empty',
                   'Средняя ставка за клик (руб.)': 'cpc',
                   'Ср. цена 1000 показов, ₽': 'cpm',
                   'Расход, ₽, с НДС': 'expense',
                   'Цена товара, ₽': 'price',
                   'Выручка, ₽': 'revenue',
                   'Выручка с заказов модели, ₽': 'revenue_model',
                   'Стоимость, ₽': 'revenue',
                   'Ставка, ₽': 'search_price_rur',
                   'Расход, ₽': 'expense',
                   'Средняя ставка, ₽': 'avrg_bid',
                   'Расход за минусом бонусов, ₽, с НДС': 'exp_bonus',
                   'Ср. цена клика, ₽': 'cpc',
                   'Средняя ставка (руб.)%!(EXTRA string=₽)': 'avrg_bid'
                   }

        dtypes = {
            'banner': 'str',
            'pagetype': 'str',
            'viewtype': 'str',
            'platfrom': 'str',
            'request_type': 'str',
            'sku': 'str',
            'name': 'str',
            'order_id': 'str',
            'order_number': 'str',
            'ozon_id': 'str',
            'ozon_id_ad_sku': 'str',
            'articul': 'str',
            'empty': 'str',
            'account_id': 'int',
            'views': 'float',
            'clicks': 'float',
            'audience': 'float',
            'exp_bonus': 'float',
            'actionnum': 'int',
            'avrg_bid': 'float',
            'search_price_rur': 'float',
            'search_price_perc': 'float',
            'price': 'float',
            'orders': 'float',
            'revenue_model': 'float',
            'orders_model': 'float',
            'revenue': 'float',
            'expense': 'float',
            'cpm': 'float',
            'ctr': 'float',
            'data': 'datetime',
            'api_id': 'str',
            'cpc': 'float'
        }

        csv_files = []
        for folder in os.listdir(path):
            csv_files += (glob.glob(os.path.join(path + folder + r'/statistics', "*.csv")))

        if len(csv_files) == 0:
            return None

        else:
            stat_data = []
            for file in csv_files:
                # data = pd.read_csv(file, sep=';')
                data = pd.read_csv(file, sep=';', header=1,
                                   skipfooter=1, engine='python'
                                   )

                data = data.dropna(axis=0, thresh=10)
                camp = pd.read_csv(file, sep=';', header=0, nrows=0).columns[-1].split(',')[0].split()[-1]

                account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
                api_id = os.path.dirname(file).split('/')[-2].split('-')[1]

                data['api_id'] = api_id
                data['account_id'] = account_id
                data['actionnum'] = camp

                data.rename(columns=columns, inplace=True)

                stat_data.append(data)

            dataset = pd.concat(stat_data, axis=0).reset_index().drop('index', axis=1)

            for col in dataset.columns:
                if dtypes[col] == 'int':
                    dataset[col] = dataset[col].astype('int', copy=False, errors='ignore')
                elif dtypes[col] == 'float':
                    dataset[col] = dataset[col].astype(str).str.replace(',', '.')
                    dataset[col] = dataset[col].astype('float', copy=False, errors='ignore')
                elif dtypes[col] == 'datetime':
                    # dataset[col] = pd.to_datetime(dataset[col], unit='D', errors='ignore')
                    dataset[col] = dataset[col].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').date())
                elif dtypes[col] == 'str':
                    dataset[col] = dataset[col].astype('str', copy=False, errors='ignore')

            dataset.replace({np.NaN: None}, inplace=True)
            dataset.replace({'nan': None}, inplace=True)

            return dataset

