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
logger.info('Starting upd parser')


def get_lost_reports(*args):

    acc_lost_reports = lost_reports_db[lost_reports_db['client_id'] == str(args[1])]

    if acc_lost_reports.shape[0] > 0:

        ozon = OzonPerformanceEcom2(account_id=args[0], client_id=args[1], client_secret=args[2])

        if ozon.auth is not None:

            reports = ozon.get_lost_reports(acc_lost_reports=acc_lost_reports, path_=config.path_)

            rep_ok = len([item for item in reports if item is not None])
            rep_lost = len([item for item in reports if item is None])
            logger.info(f"Аккаунт id {args[0]}, отчетов получено: {rep_ok}, отчетов отказано: {rep_lost}")

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


lost_reports_db = db_work_ch.get_table(table_name=config.lost_table, client=client, logger=logger)

if lost_reports_db is not None:
    if lost_reports_db.shape[0] > 0:

        accounts = db_work_ch.get_accounts(client=client, logger=logger).drop_duplicates(
            subset=['client_id', 'client_secret'], keep='last')

        threads = []
        for index, keys in accounts.iterrows():
            client_id = keys[1]
            client_secret = keys[2]
            account_id = keys[0]

            threads.append(Thread(target=get_lost_reports, args=(account_id, client_id, client_secret)))

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


# очищаем таблицу в базе
clear = db_work_ch.truncate_table(table_name=config.lost_table, client=client, logger=logger)

if clear is not None:
    # заносим новые данные
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

logger.info('End work upd parser')

