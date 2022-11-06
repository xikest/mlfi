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
        factors:pd.DataFrame = None
        data_engineered :pd.DataFrame = None
        updated_date:pd.Timestamp = None
        

class Data:
    def __init__(self, market:str='snp500'):
        self.context = Context(market=market)
        self.context = self._load_data(self.context)
        if self.context.updated_date is None: self.context = self._prepare_data(self.context)
        pass
    
    def get_data(self):
        return self.context
        
    def renew(self):
        if self._context.updated_date is not dt.datetime.today():
            self._prepare_data(self.context)
        self._save_data(self.context)
        return print("updated")
        
    def _load_data(self, context:Context):
        try:
            return FunctionPath.HDFS.load_HDFS(f'{context.market}','data.h5')
        except:
            return context
    
    def _save_data(self, context:Context):
        FunctionPath.HDFS.save_to_HDFS(context, f'{context.market}','data.h5')
        return print("saved")
    
    def _prepare_data(self, context:Context):
        tickers = [profile.ticker for profile in Profiles.load_profiles(context.market)]  #정보 객체에서 ticket만 추출하여 반환
        
        gen_prices = Prices.load_from_web(tickers)  #가격 반환을 위한 제너레이터
        gen_profiles = Profiles.load_profiles(context.market)  # 프로파일을 위한 제너레이터
        gen_ff_factors = FamaFrench.load_from_web(['F-F_Research_Data_5_Factors_2x3'])
        
        context.prices = pd.concat([price for price in gen_prices]).loc[:,'Adj Close'].unstack('ticker')
        context.factors = pd.concat([factor for factor in gen_ff_factors])
        context.profiles = pd.DataFrame([profile for profile in gen_profiles])
        # context.data_engineered = DataEngineer(context.prices, context.factors, context.profiles).get_data()      
        context.updated_date:pd.Timestamp = pd.to_datetime(dt.datetime.today())
        self._save_data(context)
        return context
    
