import pandas as pd
import numpy as np
import os
from datetime import datetime, date, timedelta
from threading import Thread
# from sqlalchemy import create_engine
import clickhouse_connect
import shutil
import config
import logger
import db_work_ch
from ozon_performance_2 import OzonPerformanceEcom2


logger = logger.init_logger()


# def get_reports(*args):
#
#     api_id = args[1].split('-')[0]
#
#     try:
#         last_date = last_dates[last_dates['api_id'] == str(api_id)]['max_date'].values[0]
#         date_from = str(np.datetime64(last_date, 'D') + np.timedelta64(1, 'D'))
#         # date_from = str(last_date + timedelta(days=1))
#
#     except (IndexError, KeyError, ValueError):
#         date_from = str(date.today() - timedelta(days=30))
#
#     date_to = str(date.today() - timedelta(days=1))
#
#     ozon = OzonPerformanceEcom2(account_id=args[0], client_id=args[1], client_secret=args[2])
#     ozon.camp_lim = config.CAMPS_LIM
#     ozon.day_lim = config.DAYS_LIM
#
#     if ozon.auth is not None:
#         logger.info(f"Аккаунт id {args[0]}: {len(ozon.get_campaigns(active_only=config.ONLY_ACTIVE))} кампаний")
#
#         stat = ozon.collect_statistics(date_from=date_from, date_to=date_to, active_only=config.ONLY_ACTIVE)
#
#         if len(stat) > 0:
#
#             rep_ok = len([item for item in stat if item is not None])
#             rep_lost = len([item for item in stat if item is None])
#             logger.info(f"Аккаунт id {args[0]}, отчетов получено: {rep_ok}, отчетов отказано: {rep_lost}")
#
#             ozon.save_statistics(st_camp=stat, path_=config.path_)
#
#         else:
#             logger.info(f"Аккаунт id {args[0]}: нет кампаний для сбора статистики")


def get_reports(*args):

    api_id = args[1].split('-')[0]

    try:
        last_date = last_dates[last_dates['api_id'] == str(api_id)]['max_date'].values[0]
        date_from = str(np.datetime64(last_date, 'D') + np.timedelta64(1, 'D'))
        # date_from = str(last_date + timedelta(days=1))

    except (IndexError, KeyError, ValueError):
        date_from = str(date.today() - timedelta(days=30))

    date_to = str(date.today() - timedelta(days=1))

    ozon = OzonPerformanceEcom2(account_id=args[0], client_id=args[1], client_secret=args[2])
    ozon.camp_lim = config.CAMPS_LIM
    ozon.day_lim = config.DAYS_LIM

    if ozon.auth is not None:
        logger.info(f"Аккаунт id {args[0]} найдено кампаний: {len(ozon.get_campaigns(active_only=config.ONLY_ACTIVE))}")

        reports = ozon.get_reports(date_from=date_from, date_to=date_to, active_only=config.ONLY_ACTIVE, path_=config.path_)

        rep_ok = len([item for item in reports if item is not None])
        rep_lost = len([item for item in reports if item is None])
        logger.info(f"Аккаунт id {args[0]}, отчетов получено: {rep_ok}, отчетов отказано: {rep_lost}")

        # print(ozon.lost)
        if len(ozon.lost) > 0:
            lost_reports = pd.DataFrame(ozon.lost)
            lost_reports.to_csv(f'./lost/{ozon.account_id}-{ozon.client_id}.csv', index=False, encoding='utf-8',
                                sep=';')


client = clickhouse_connect.get_client(
    interface='https',
    host=config.CH_HOST,
    port=config.CH_PORT,
    username=config.CH_USER,
    password=config.CH_PASSWORD,
    database=config.CH_DB_NAME,
    secure=True,
    verify=True,
    ca_cert=config.CH_CA_CERTS
)

accounts = db_work_ch.get_accounts(client=client, logger=logger).drop_duplicates(subset=['client_id', 'client_secret'], keep='last')
last_dates = db_work_ch.get_last_dates(table_name=config.stat_table, client=client, logger=logger)

threads = []
for index, keys in accounts.iterrows():
    client_id = keys[1]
    client_secret = keys[2]
    account_id = keys[0]

    threads.append(Thread(target=get_reports, args=(account_id, client_id, client_secret)))


print(threads)

# запускаем потоки
for thread in threads:
    thread.start()

# останавливаем потоки
for thread in threads:
    thread.join()


df = OzonPerformanceEcom2.make_dataset(path=config.path_)

if df is None:
    logger.info("no downloaded files")

else:
    if df.shape[0] == 0:
        logger.info("no stat data for period")

    else:
        if config.upl_into_db == 1:
            upload = db_work_ch.insert_data(dataset=df, table_name=config.stat_table, client=client, logger=logger)

            if upload is not None:
                logger.info("Upload to ch_db successful")
            else:
                logger.error('Upload to ch_db error')

        else:
            logger.info('Upl to db canceled')

    if config.delete_files == 1:
        try:
            shutil.rmtree(config.path_)
            logger.info('Files (folder) deleted')
        except OSError as e:
            logger.error("Error: %s - %s." % (e.filename, e.strerror))
    else:
        logger.info('Delete canceled')


lost_data = OzonPerformanceEcom2.make_lost_dataset(path=config.lost_folder)

if lost_data is None:
    logger.info('No lost data')
else:
    upl = db_work_ch.insert_data(dataset=lost_data, table_name=config.lost_table, client=client, logger=logger)
    if upl is not None:
        try:
            shutil.rmtree(config.lost_folder)
            logger.info('Files (folder) deleted')
        except OSError as e:
            logger.error("Error: %s - %s." % (e.filename, e.strerror))


