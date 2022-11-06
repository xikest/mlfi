from typing import Generator, List
import pandas as pd
import pandas_datareader.data as web 
# from pandas_datareader import wb
import FinanceDataReader as fdr  # pip install -U finance-datareader
from tqdm import tqdm
# import asyncio


class Prices:
    # @staticmethod
    # async   def load_from_web(symbols:List[str]=['AAPL'], start_date='2020-1-1', end_date='2022-12-31') -> pd.DataFrame:
    #             for symbol in symbols: 
    #                 try:
    #                     df = web.DataReader(symbol, 'yahoo', start=start_date, end=end_date)
    #                     df['Symbol']=symbol
    #                     yield  df.reset_index().set_index(['Date','Symbol']).loc[:,['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']] #yahoo: 'Adj Close'
    #                     await asyncio.sleep(1)
    #                 except Exception as e:
    #                     print(f'{symbol},prices: {e}')
    #                     pass
    @staticmethod
    def load_from_web(symbols:List[str]=['AAPL'], start_date='2020-1-1', end_date='2022-12-31') -> pd.DataFrame:
        """
        pandaDatareader에서 데이터를 받아온다.
        
        Args:
            symbol (str): 주식의 심볼
            start_date (_type_): 조회 시작일
            end_date (_type_): 조회 종료일
        Yields:
            pd.DataFrame: 요청된 symbols의 adj close 데이터를 반환한다. 
            
        Data.load_from_web()
        >>
        Open	High	Low	Close	Volume	Adj Close
        Date	Symbol						
        2019-12-31	AAPL	72.482498	73.419998	72.379997	73.412498	100805600.0	72.039879
        2020-01-02	AAPL	74.059998	75.150002	73.797501	75.087502	135480400.0	73.683571
        2020-01-03	AAPL	74.287498	75.144997	74.125000	74.357498	146322800.0	72.967232
        2020-01-06	AAPL	73.447502	74.989998	73.187500	74.949997	118387200.0	73.548637
        2020-01-07	AAPL	74.959999	75.224998	74.370003	74.597504	108872000.0	73.202744
        """
        
        
        for symbol in tqdm(symbols): 
            try:
                df = web.DataReader(symbol, 'yahoo', start=start_date, end=end_date)
                df['ticker']=symbol
                yield  df.reset_index().set_index(['Date','ticker']).loc[:,['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']] #yahoo: 'Adj Close'
            except Exception as e:
                print(f'{symbol},prices: {e}')
                pass
        # elif 'famafrench': 
        #     yield {symbol : web.DataReader(symbol, source, start=start_date, end=end_date)[0] for symbol in symbols}

    # @staticmethod
    # async def load_from_fdr(symbols:List[str]=['AAPL'], start='2020-1-1', end='2022-12-31') -> pd.DataFrame:
    #             for symbol in symbols: 
    #                 try:
    #                     df = fdr.DataReader(symbol, start=start_date, end=end_date)
    #                     df['Symbol']=symbol
    #                     yield  df.reset_index().set_index(['Date','Symbol']).loc[:,['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']] #yahoo: 'Adj Close'
    #                     await asyncio.sleep(1)
    #                 except Exception as e:
    #                     print(f'{symbol},prices: {e}')
    #                     pass
                        

    @staticmethod
    def load_from_fdr(symbols:List[str]=['AAPL'], start_date='2020-1-1', end_date='2022-12-31') -> pd.DataFrame:
        """
        FinacialDatareader에서 데이터를 받아온다.
        Args:
                    symbol (str): 주식의 심볼
                    source (str): 데이터의 소스: yahoo와 FF 데이터_
                    start_date (_type_): 조회 시작일
                    end_date (_type_): 조회 종료일
        Yields:
                    pd.DataFrame: 요청된 symbols의 adj close 데이터를 반환한다. 
        """
        for symbol in tqdm(symbols): 
            try:
                df = fdr.DataReader(symbol, start=start_date, end=end_date)
                df['ticker']=symbol
                yield  df.reset_index().set_index(['Date','ticker']).loc[:,['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']] #yahoo: 'Adj Close'
            except Exception as e:
                print(f'{symbol},prices: {e}')
                pass
            
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