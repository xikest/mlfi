from typing import Optional, Dict
from dataclasses  import dataclass
import pandas as pd
import datetime as dt
import tqdm
from .providers import Profiles, Prices, FamaFrench, DataEngineer
from .functions_for_data import FunctionPath

 
@dataclass
class Context:
        market:Optional[str]=None
        prices:pd.DataFrame = None
        volumes: pd.DataFrame = None
        profiles:pd.DataFrame = None
        factors:pd.DataFrame = None
        data_engineered :Dict[str,pd.DataFrame] = None
        updated_date:pd.Timestamp = None
        

class Data:
    def __init__(self, market:str='snp500'):
        self._context = Context(market=market)
        self._context = self._load_data(self._context)
        if self._context.updated_date is None: self._context = self._prepare_data(self._context)
        pass
    
    @property
    def market(self):
        return self._context.market
    
    @property
    def prices(self):
        return self._context.prices
    
    @property
    def volumes(self):
        return self._context.volumes  
    
    @property
    def profiles(self):
        return self._context.profiles  
    
    @property
    def factors(self):
        return self._context.factors  
    
    @property
    def updated_date(self):
        return self._context.updated_date  
    
    @property
    def data_engineered(self):
        return self._context.data_engineered  
    
    def update(self):  # 데이터 갱신 차후 구현
        pass
        
    def renew(self): # 데이터 새로 받기
        if self._context.updated_date is not dt.datetime.today().strftime('%Y-%m-%d'):
            self._prepare_data(self._context)
        self._save_data(self._context)
        return print("updated")
        
    def _load_data(self, context:Context): #불러오기
        try:

            data = FunctionPath.Pickle.load_from_pickle(f'{context.market}')
            print(f'{context.market} loaded')
            return data
        except:
            print(f'{context.market} load fail')
            return context
    
    def _save_data(self, context:Context): # 저장
        context.updated_date:pd.Timestamp = dt.datetime.today().strftime('%Y-%m-%d')
        FunctionPath.Pickle.save_to_pickle(context, f'{context.market}')
        print(f'{context.market} saved')
        return print("saved")
    
    def _prepare_data(self, context:Context):
        # tickers = [profile.ticker for profile in Profiles.load_profiles(context.market)]  #삭제
        
        #프로파일 정보 저장
        gen_profiles = Profiles.load_profiles(context.market)  # 프로파일을 위한 제너레이터
        context.profiles = pd.DataFrame([profile for profile in gen_profiles]).set_index('ticker')
        
        # 중간 데이터 저장 (프로파일)
        self._save_data(context)
        
        # 가격 데이터 저장
        tickers = context.profiles.reset_index().loc[:,'ticker'] #정보 객체에서 ticket만 추출하여 반환
        data_src = context.profiles.reset_index().loc[:,'data_src'] 
        
        gen_prices = Prices.load_from_web(tickers, data_src= data_src,  start='2000-1-1', end='2022-12-31')  #가격 반환을 위한 제너레이터
        
        
        prices_volumes = pd.concat([price for price in tqdm(gen_prices)]).loc[:,['Adj Close', 'Volume']].unstack('ticker')
        context.prices = prices_volumes.loc[:,'Adj Close']  # 가격 
        context.volumes = prices_volumes.loc[:,'Volume']  #거래량
        # pd.concat([price for price in gen_prices]).loc[:,'Adj Close'].unstack('ticker')  #삭제
        
        # 중간 데이터 저장 (가격데이터)
        self._save_data(context)
        
        #factor 데이터 저장
       
        gen_ff_factors = FamaFrench.load_from_web([context.profiles.loc[:,'market_factors'][0]])
        context.factors = pd.concat([factor for factor in gen_ff_factors])
        # 중간 데이터 저장 (팩터 데이터)
        self._save_data(context)
        # context.profiles = pd.DataFrame([profile for profile in gen_profiles]).set_index('ticker') #삭제
        
        # 데이터 엔지니어링 작업
        
        context.data_engineered ={period: DataEngineer(context.prices, context.factors, context.profiles, period=period).get_data() for period in ['w', 'm']}
        
        # 업데이트 일자 기록     
        self._save_data(context)
        return context
    
