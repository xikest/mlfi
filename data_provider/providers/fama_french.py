from typing import List
import pandas as pd
import pandas_datareader.data as web 
# from pandas_datareader import wb
from tqdm import tqdm
# import asyncio

class FamaFrench:
        @staticmethod
        def load_from_web(symbols:List[str]=['F-F_Research_Data_5_Factors_2x3'], start_date='2020-1-1', end_date='2022-12-31') -> pd.DataFrame:
            for symbol in tqdm(symbols): 
                    try:
                        df = web.DataReader(symbol, 'famafrench', start=start_date, end=end_date)
                        yield df[0]
                    except Exception as e:
                        print(f'F-F_Research_Data: {e}')
                        pass