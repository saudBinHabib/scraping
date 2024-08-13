from bs4 import BeautifulSoup
from collections import namedtuple
from pathlib import Path

import logging
import os
import pandas as pd
import requests
import wget
import zipfile

from database import MySqliteDatabase

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('../logs/Scraping.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class Scraping:

    __DATA_DIRECTORY = '../data/'
    __DATA_ZIP_FILE = '../data/events_data.zip'
    __PARQUET_FILE_ADDRESS = '../parquet/complete.parquet.gzip'
    WEB_URL = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'

    def __int__(self):
        pass

    def get_list_content(self) -> list:
        html_content = requests.get(self.WEB_URL).content
        soup = BeautifulSoup(html_content, 'html.parser')
        text_content = soup.find_all(text=True)[0].split('\n')

        Type = namedtuple('Type', ['size', 'ids', 'urls'])

        types = list()
        for row in text_content:
            data = row.split()
            if data:
                types.append(Type(data[0], data[1], data[2]))
        return types

    def download_file(self, file_url: str):
        wget.download(file_url, self.__DATA_ZIP_FILE)
        logger.info('File Downloaded: {} '.format(file_url))

    def unzip_file(self):
        with zipfile.ZipFile(self.__DATA_ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(self.__DATA_DIRECTORY)
        logger.info('File Unzipped.')

    def read_csv_file(self):
        data_directory = Path(self.__DATA_DIRECTORY)

        csv_files = data_directory.glob('*.csv')
        df = pd.DataFrame()
        try:
            for data_file in csv_files:
                df_sub = pd.read_csv(data_file, sep='\t')
                columns_name = ['col_' + str(counter) for counter in range(df_sub.shape[1])]
                df_sub = pd.read_csv(data_file, names=columns_name, sep='\t')
                df = pd.concat([df, df_sub], ignore_index=True)
            logger.info('Data Frame Created.')
            return df
        except IndexError:
            logger.error('Data Files Are broken.')
            raise RuntimeError('data files are broken.')

    def parquet_file_exists(self) -> bool:
        return True if os.path.exists(self.__PARQUET_FILE_ADDRESS) else False

    def get_parquet_files_data(self):
        df_parquet = pd.read_parquet(self.__PARQUET_FILE_ADDRESS)
        logger.info('Parquet File reading completed.')
        return df_parquet

    def empty_data_folder(self):
        data_directory = Path(self.__DATA_DIRECTORY)
        all_files = data_directory.glob('*')
        for file in all_files:
            os.remove(file)
        logger.info('Deleted all the files from the data directory.')

    def delete_duplicates_and_save_it(self, df_latest, df_parquet):
        df_complete = df_parquet.append(df_latest)
        df_complete.drop_duplicates(inplace=True)
        df_complete.to_parquet(self.__PARQUET_FILE_ADDRESS, compression='gzip')

    def write_parquet_file(self, df_parquet):
        df_parquet.to_parquet(self.__PARQUET_FILE_ADDRESS, compression='gzip')


if __name__ == "__main__":

    scraper = Scraping()

    db = MySqliteDatabase()
    db.connect_db()

    # if database do not exist, first create db structure.
    if not os.path.exists('database/file.db'):
        db.create_files_table()

    tuples = scraper.get_list_content()

    # as we only want events data file and that file contains gkg in the end, so we check the filter.
    events_data_url = [url_tuple.urls for url_tuple in tuples if 'gkg' in url_tuple.urls][0]

    if db.file_existed(events_data_url):
        logger.info('Data is already Stored.')
    else:
        db.insert_file(events_data_url)
        scraper.download_file(events_data_url)
        scraper.unzip_file()
        df_current_data = scraper.read_csv_file()
        scraper.empty_data_folder()
        if scraper.parquet_file_exists():
            df_parquet_date = scraper.get_parquet_files_data()
            scraper.delete_duplicates_and_save_it(df_current_data, df_parquet_date)
        else:
            scraper.write_parquet_file(df_current_data)

    logger.info('Completed The Whole Process.')
    db.close_connection()
