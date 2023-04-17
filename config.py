import os
import sys
from datetime import date

# PG_HOST = os.environ.get('ECOMRU_PG_HOST', None)
# PG_PORT = os.environ.get('ECOMRU_PG_PORT', None)
# PG_SSL_MODE = os.environ.get('ECOMRU_PG_SSL_MODE', None)
# PG_DB_NAME = os.environ.get('ECOMRU_PG_DB_NAME', None)
# PG_USER = os.environ.get('ECOMRU_PG_USER', None)
# PG_PASSWORD = os.environ.get('ECOMRU_PG_PASSWORD', None)
# PG_target_session_attrs = 'read-write'

# PG_DB_PARAMS = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"

CH_HOST = os.environ.get('ECOMRU_CH_HOST', None)
CH_DB_NAME = os.environ.get('ECOMRU_CH_DB_NAME', None)
CH_USER = os.environ.get('ECOMRU_CH_USER', None)
CH_PASSWORD = os.environ.get('ECOMRU_CH_PASSWORD', None)
CH_PORT = os.environ.get('ECOMRU_CH_PORT', None)


using_db = 'clickhouse'
data_folder = './data'
CAMPS_LIM = 1
DAYS_LIM = 3
ONLY_ACTIVE = True

delete_files = 1
upl_into_db = 1

stat_table = 'ozon_perf_statistics_full'

# создаем рабочую папку, если еще не создана
if not os.path.isdir(data_folder):
    os.mkdir(data_folder)
# путь для сохранения файлов в рабочей папке
path_ = f'{data_folder}/{str(date.today())}/'
if not os.path.isdir(path_):
    os.mkdir(path_)

if sys.platform == 'linux':
    CH_CA_CERTS = "/usr/local/share/ca-certificates/Yandex/YandexCA.crt"
elif sys.platform == 'win32':
    CH_CA_CERTS = 'C:/Users/FED/.clickhouse/YandexCA.crt'
else:
    CH_CA_CERTS = None



