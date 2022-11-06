from typing import Optional
from dataclasses  import dataclass
import pandas as pd
import datetime as dt
from .providers import Profiles, Prices, FamaFrench, DataEngineer
from .functions_for_data import FunctionPath

 
@dataclass
class Context:
        market:Optional[str]=None
        prices:pd.DataFrame = None
        profiles:pd.DataFrame = None
        factos:pd.DataFrame = None
        data :pd.DataFrame = None
        descr:Optional[str] = None
        updated_date:pd.Timestamp = None
        

class Data:
    def __init__(self, market:str='snp500'):
        self._market = market
        self._context:Context = None
        prepared = self.load_data()
        if prepared: pass
        else: self._prepare_data()
        pass
    
    def update(self):
        if self._context.updated_date is not dt.datetime.today():
            self._prepare_data()
        pass
        
    def load_data(self):
        SUCESS, FAIL = True, False
        try:
            self._context = FunctionPath.HDFS.load_HDFS(f'{self._market}','data.h5')
            return SUCESS
        except:
            return FAIL
    
    def save_data(self):
        FunctionPath.HDFS.save_to_HDFS(self._context, f'{self._market}','data.h5')
        pass
    
    def _prepare_data(self):
        tickers = [info.ticker for info in Profiles.load_info(self._market)]  #정보 객체에서 ticket만 추출하여 반환
        gen_prices = Prices.load_from_web(tickers)  #가격 반환을 위한 제너레이터
        gen_profiles = Profiles.load_info(self._market)  # 프로파일을 위한 제너레이터
        gen_ff_factors = FamaFrench.load_from_web(['F-F_Research_Data_5_Factors_2x3'])
        
        self._context.prices = pd.concat([price for price in gen_prices]).loc[:,'Adj Close'].unstack('ticker')
        self._context.factors = pd.concat([factor for factor in gen_ff_factors])
        self._context.profiles = pd.DataFrame([info.profiles for info in gen_profiles])
        self._context.data_engineered = DataEngineer(self._context.prices, self._context.factors, self._context.profiles).get_data()          
        self._context.updated_date:pd.Timestamp = pd.to_datetime(dt.datetime.today())
        pass
    
