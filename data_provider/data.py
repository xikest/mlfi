from typing import Optional, Dict, List
from dataclasses  import dataclass
import pandas as pd
import datetime as dt
import tqdm
from .providers import Profiles, Prices, FamaFrench, DataEngineer
from .functions_for_data import FunctionPath
import pandas as pd
 
@dataclass
class Context:
        label:Optional[str]=None
        prices:pd.DataFrame = None
        volumes: pd.DataFrame = None
        profiles:pd.DataFrame = None
        factors:pd.DataFrame = None
        data_engineered :Dict[str,pd.DataFrame] = None
        prices_engineered:pd.DataFrame = None
        updated_date:pd.Timestamp = None
        



class BasicData:
    def __init__(self, label:str='snp500'):
        self._context = Context(label=label)
        self._context = self._load_data(self._context)
        if self._context.updated_date is None: self._context = self._prepare_data(self._context)
        pass
    
    @property
    def label(self):
        return self._context.label
    
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

            data = FunctionPath.Pickle.load_from_pickle(f'{context.label}')
            print(f'{context.label} loaded')
            return data
        except:
            print(f'{context.label} load fail')
            return context
    
    def _save_data(self, context:Context): # 저장
        context.updated_date:pd.Timestamp = dt.datetime.today().strftime('%Y-%m-%d')
        FunctionPath.Pickle.save_to_pickle(context, f'{context.label}')
        print(f'{context.label} saved')
        return print("saved")
    
    def _prepare_data(self, context:Context):
        # tickers = [profile.ticker for profile in Profiles.load_profiles(context.label)]  #삭제
        
        #프로파일 정보 저장
        gen_profiles = Profiles.load_profiles(context.label)  # 프로파일을 위한 제너레이터
        context.profiles = pd.DataFrame([profile for profile in gen_profiles]).set_index('ticker')
        
        # 중간 데이터 저장 (프로파일)
        # self._save_data(context)
        # 가격 데이터 저장
        tickers = context.profiles.reset_index().loc[:,'ticker'] #정보 객체에서 ticket만 추출하여 반환
        data_src = context.profiles.reset_index().loc[:,'data_src'] 
        
        gen_prices = Prices.load_from_web(tickers, data_src= data_src,  start='2010-1-1', end='2022-12-31')  #가격 반환을 위한 제너레이터
        
        print('start prices')
       
        prices_volumes = pd.concat([price for price in gen_prices]).loc[:,['Adj Close', 'Volume']].unstack('ticker')
        context.prices = prices_volumes.loc[:,'Adj Close']  # 가격 
        context.volumes = prices_volumes.loc[:,'Volume']  #거래량
        # pd.concat([price for price in gen_prices]).loc[:,'Adj Close'].unstack('ticker')  #삭제
        
        # 중간 데이터 저장 (가격데이터)
        # self._save_data(context)
        
        #factor 데이터 저장
        print('start factors')
       
        gen_ff_factors = FamaFrench.load_from_web([context.profiles.loc[:,'market_factors'][0]])
        context.factors = pd.concat([factor for factor in gen_ff_factors])
        # 중간 데이터 저장 (팩터 데이터)
        # self._save_data(context)
        
        # 데이터 엔지니어링 작업
        print('start engineering')
        de = DataEngineer(context.prices, context.volumes, context.factors, context.profiles)
        context.data_engineered ={period: de.get_data_engineered(period=period) for period in ['w', 'm']}
        context.prices_engineered = de.get_prices_engineered()
        
        # 업데이트 일자 기록     
        self._save_data(context)
        print('fin')
        return context
    
    
class CustomizingData(BasicData):
    def __init__(self, src_label:str ='etf_us'):
        super().__init__(src_label)
        pass
        
        
    def make_dataset(self, cols:List[str]=['XOM', 'AAPL'], dataset_label = 'customDataset'):
        idx = pd.IndexSlice
        context = Context( label = dataset_label,
                            profiles = self._context.profiles.loc[cols,:],
                            prices = self._context.prices.loc[:, cols],
                            volumes = self._context.volumes.loc[:,cols],
                            factors= self._context.factors,
                            data_engineered ={'w': self._context.data_engineered.get('w').loc[idx[cols,:],:],
                                                            'm': self._context.data_engineered.get('w').loc[idx[cols,:],:]},
                            updated_date = self._context.updated_date
                            )
        self._save_data(context)
        return context