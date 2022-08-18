from fuzzywuzzy import process
from dask_snowflake import to_snowflake
import snowflake.connector as sf
import dask.dataframe as dd
import pandas as pd
import time
from dask.distributed import Client
import dask.dataframe as dd

import time
import psutil
import sys
import dask.utils as util

class SFdatabase:
    
    secret_keys = {
        'user':'SVC_PYTHONIDE', 
        'password':'Welcome12345', 
        'account':'ti97672.east-us-2.azure', 
        'warehouse': 'DEV', 
        'database':'DEV_PROVIDER_REGISTRY_CLONE_GALAXE', 
        'schema':'DBO'
        }


    def __init__(self):
        self.conn = sf.connect(
            user=SFdatabase.secret_keys['user'],
            password=SFdatabase.secret_keys['password'],
            account=SFdatabase.secret_keys['account'],
            warehouse=SFdatabase.secret_keys['warehouse'],
            database=SFdatabase.secret_keys['database'],
            schema=SFdatabase.secret_keys['schema']
        )
        self.cur = self.conn.cursor()


    def readDB(self, sm_records='', pr_records='', state=''):
        # Standard mapper
        if state.strip():
             state=state
        else :
            state='NY'

        self.sm_df = self.cur.execute(
            f"SELECT {sm_records} IMPORTID, FIRSTNAME, LASTNAME FROM DBT_RMURAHARISETTY.STANDAREDMAPPER \
                WHERE State='NY' AND PROVIDERTYPE = 1  \
                AND IMPORTID NOT IN (SELECT IMPORTID FROM PROVIDERSTANDARDMAPPER_02)"
            ).fetch_pandas_all()
        self.sm_df['FULLNAME'] = self.sm_df['FIRSTNAME'] + ' ' + self.sm_df['LASTNAME']

        # Provider registry
        self.pr_df = self.cur.execute(
            f"SELECT {pr_records} PROVIDER.PROVIDERID, FIRSTNAME, LASTNAME, PROVIDERADDRESSID FROM PROVIDER \
                INNER JOIN PROVIDERADDRESS ON PROVIDER.PROVIDERID = PROVIDERADDRESS.PROVIDERID  \
                WHERE State='NY' AND PROVIDERTYPE = 1"
            ).fetch_pandas_all()
        self.pr_df['FULLNAME'] = self.pr_df['FIRSTNAME'] + ' ' + self.pr_df['LASTNAME']


    def fuzzy_merge(self, df_1, df_2, key1, key2, threshold=90, limit=1):
        """
        :param df_1: the left table to join
        :param df_2: the right table to join
        :param key1: key column of the left table
        :param key2: key column of the right table
        :param threshold: how close the matches should be to return a match, based on Levenshtein distance
        :param limit: the amount of matches that will get returned, these are sorted high to low
        :return: dataframe with boths keys and matches
        """
        s = df_2[key2].tolist()

        m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))
        df_1['matches'] = m
        print(df_1['matches'])
   
        m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
        df_1['matches'] = m2
        print(df_1['matches'])
        return df_1


    def matchNames(self):
        self.final_df=self.fuzzy_merge(self.sm_df, self.pr_df, 'FULLNAME', 'FULLNAME', 80)

        df_filter = self.final_df[self.final_df['matches'].isna() == False]
        print(df_filter)
        y = zip(df_filter['matches'], df_filter['FULLNAME'])
        x = dict(zip( df_filter['matches'],df_filter['FULLNAME']))
        self.pr_df['FULLNAME_bkp']= self.pr_df['FULLNAME'].replace(x)
        combined_df = pd.merge(self.final_df, self.pr_df, how='left', left_on='FULLNAME', right_on='FULLNAME_bkp')
        
        combined_df.drop(
            axis=1 ,
            columns=['matches', 'FULLNAME_bkp', 'FULLNAME_x', 'FULLNAME_y', 
                'FIRSTNAME_x', 'FIRSTNAME_y', 'LASTNAME_x', 'LASTNAME_y'], 
            inplace=True
            )
        combined_df.dropna(subset=['PROVIDERID'], how='any', inplace=True)
        combined_df['CONTAINERID'] = 1
        combined_df['DATASETID'] = 5
        combined_df['CONFIDENCE_SCORE'] = 80
        combined_df['STATUS'] = 0
        combined_df['MATCHEDON'] = 0
        print(combined_df)
        self.final_df = combined_df


    def toSnowflake(self):
        # write_pandas(self.conn, self.final_df, 'PROVIDERSTANDARDMAPPER')
        to_snowflake(dd.from_pandas(data=self.final_df, npartitions=1), 'providerstandardmapper', self.secret_keys)

    def close(self):
        self.cur.close()
        self.conn.close()

    def getResultSet(self):
        return self.final_df

if __name__=='__main__':
    num_core=psutil.cpu_count(logical=False)
    if len(sys.argv)>1:
        num_core=sys.argv[1]
    start = time.time()
    print(num_core)
    # client = Client()
    db = SFdatabase()
    start = time.time()
    print(f'Read Start time: {float((time.time() - start)) / 60} minutes')
    print('Reading data...')
    db.readDB('', '', '')
    print(f'Read END time: {float((time.time() - start)) / 60} minutes')
    print(len(db.sm_df.index))
    print(len(db.pr_df.index))
    db.sm_df.to_parquet('SMdata_NY.parquet',compression='gzip')
    db.pr_df.to_parquet('PRData_NY.parquet',compression='gzip')
    
    # print('Matching...')
    # print(time.localtime())
    # client.submit(db.matchNames())
    # db.matchNames()
    # print(f'Fuzzy Match END time: {float((time.time() - start)) / 60} minutes')
    # print(len(db.final_df.index))
    # print('Sending to snwoflake...')
    # db.toSnowflake()
    db.close()
    print('Done!')
    print(f'Total process time: {float((time.time() - start)) / 60} minutes')

# # RUNTIME # #
# 10 SM against 1K PR: 0.2 minutes
# 10 SM against 10k PR: 0.3 minute
# 100 SM against 10k PR: 2.2 minutes