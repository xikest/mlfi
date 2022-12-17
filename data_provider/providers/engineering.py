
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from talib import RSI, BBANDS, MACD, ATR

from .function_for_engineering.returns import Returns
from .function_for_engineering.factors import MarketFactors, MomentumFactors, DateIndicators, LaggedReturns, HoldingPeriodReturns, DynamicSizeFactors, SectorFactors, DummyVariables


class DataEngineer:
    """ 데이터를 가공한다.
    """
    def __init__(self, dfPrices:pd.DataFrame, dfVolumes:pd.DataFrame,  dfFactors:pd.DataFrame, dfprofiles:pd.DataFrame):
        """_summary_

        Args:
            dfPrices (pd.DataFrame): 계산한 가격 데이터이다. 인덱스는 멀티 인덱스이고, index(Date, Symbol), columns[price]
            dfFactors (F): 파머 프렌치 팩터 데이터이다. F-F_Research_Data_5_Factors_2x3
        """
        # self._data_engineered = None
        # self._prices_engineered = None
        
        self._dfPrices = dfPrices
        self._dfVolumes = dfVolumes
        self._dfFactors = dfFactors
        self._dfprofiles = dfprofiles
        pass
    
    def get_data_engineered(self, period='m'):
        # if self._data_engineered is None:  
        return self._engineering_data(self._dfPrices, self._dfprofiles, self._dfFactors, period)
        # return self._data_engineered
        
    def get_prices_engineered(self):
        # if self._prices_engineered is None:  self._prices_engineered = 
        return self._engineering_prices(self._dfPrices, self._dfVolumes)
        
        
    def _engineering_data(self, dfPrices:pd.DataFrame, dfprofiles:pd.DataFrame=None, dfFactors:pd.DataFrame=None, period:str='m'):
        dfRtn = Returns(dfPrices, period).get_data()
        if dfprofiles.loc[:,'market_factors'][0] is not None:  
            dfRtn = MarketFactors(dfRtn, dfFactors, period).get_data()  #계산을 위해 마켓 팩커 값이 필요함
        dfRtn = MomentumFactors(dfRtn, period).get_data()
        dfRtn = DateIndicators(dfRtn).get_data()
        dfRtn = LaggedReturns(dfRtn, period).get_data()
        dfRtn = HoldingPeriodReturns(dfRtn, period).get_data()
        if  dfprofiles.loc[:,'enable_profile_engineering'][0] == True :  # 프로파일의 시총 등의 값이 필요 함, df의 값은 모두 같은 값으로 하나만 확인하면 됨
            dfRtn = DynamicSizeFactors(dfRtn,dfPrices, dfprofiles).get_data()  
            dfRtn = SectorFactors(dfRtn, dfprofiles).get_data()  
        dfRtn = DummyVariables(dfRtn).get_data() 
        return dfRtn 
    
    def _engineering_prices(self, dfPrices:pd.DataFrame, dfVolumes:pd.DataFrame) -> pd.DataFrame:
        MONTH = 21
        YEAR = 12 * MONTH
        idx = pd.IndexSlice
        prices = pd.DataFrame(dfPrices.stack(), columns=['close']).assign(volume=dfVolumes.stack().div(1000)) .swaplevel() .sort_index()
        
        min_obs = 2 * YEAR
        nobs = prices.groupby(level='ticker').size()
        keep = nobs[nobs > min_obs].index
        prices = prices.loc[idx[keep, :], :]
        
        prices['dollar_vol'] = prices[['close', 'volume']].prod(axis=1)
        prices['dollar_vol_1m'] = (prices.dollar_vol.groupby('ticker')
                                .rolling(window=21)
                                .mean()).values
        prices['dollar_vol_rank'] = (prices.groupby('Date')
                                    .dollar_vol_1m
                                    .rank(ascending=False))
        
        
        #RSI 생성
        prices['rsi'] = prices.groupby(level='ticker').close.apply(RSI)
        
        ##bb band 생성
        prices = (prices.join(prices
                      .groupby(level='ticker')
                      .close
                      .apply(compute_bb)))
        
        prices['bb_high'] = prices.bb_high.sub(prices.close).div(prices.bb_high).apply(np.log1p)
        prices['bb_low'] = prices.close.sub(prices.bb_low).div(prices.close).apply(np.log1p)
        
        ## MACD 생성
        prices['macd'] = (prices
                  .groupby('ticker', group_keys=False)
                  .close
                  .apply(compute_macd))
        
        prices.macd.describe(percentiles=[.001, .01, .02, .03, .04, .05, .95, .96, .97, .98, .99, .999]).apply(lambda x: f'{x:,.1f}')
        
        lags = [1, 5, 10, 21, 42, 63]
        returns = prices.groupby(level='ticker').close.pct_change()
        percentiles=[.0001, .001, .01]
        percentiles+= [1-p for p in percentiles]
        returns.describe(percentiles=percentiles).iloc[2:].to_frame('percentiles').style.format(lambda x: f'{x:,.2%}')
        q = 0.0001
        
        # lag 수익률 생성
        for lag in lags:
            prices[f'return_{lag}d'] = (prices.groupby(level='ticker').close
                                        .pct_change(lag)
                                        .pipe(lambda x: x.clip(lower=x.quantile(q),
                                                            upper=x.quantile(1 - q)))
                                        .add(1)
                                        .pow(1 / lag)
                                        .sub(1)
                                        )
        for t in [1, 2, 3, 4, 5]:
            for lag in [1, 5, 10, 21]:
                prices[f'return_{lag}d_lag{t}'] = (prices.groupby(level='ticker')
                                                [f'return_{lag}d'].shift(t * lag))
        
        for t in [1, 5, 10, 21]:
            prices[f'target_{t}d'] = prices.groupby(level='ticker')[f'return_{t}d'].shift(-t)
        
        prices['year'] = prices.index.get_level_values('Date').year
        prices['month'] = prices.index.get_level_values('Date').month
        
        return prices

def compute_bb(close):
    high, mid, low = BBANDS(close, timeperiod=20)
    return pd.DataFrame({'bb_high': high, 'bb_low': low}, index=close.index)


def compute_macd(close):
    macd = MACD(close)[0]
    return (macd - np.mean(macd))/np.std(macd)