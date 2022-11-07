
import pandas as pd
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm


class Factors:
        def __init__(self, dfRtn:pd.DataFrame, period:str='M'):
            """
            market Factor 데이터를 계산한다

            Args:
                dfRtn (pd.DataFrame): 수익률 데이터 
            """
            self._data = None
            self._dfRtn = dfRtn
            self._period = period
            
        def get_data(self) -> pd.DataFrame:
            """
            계산된 수익률 데이터를 반환한다.

            Returns:
                pd.DataFrame: Factor가 추가된 수익률 데이터
            """
            if self._data is None: self._data = self._calculate_factors() 
            return self._data 
            
        def _calculate_factors(self) -> pd.DataFrame:
            """Factor를 계산한다.

            Args:
                dfRtn (pd.DataFrame): 수익률 데이터

            Returns:
                pd.DataFrame: Factor가 추가된 수익률 데이터
            """
            return None
            
# ===============================================
# market factors 
# ===============================================
class MarketFactors(Factors):
    def __init__(self, dfRtn:pd.DataFrame, dfFactor:"F-F_Research_Data_5_Factors_2x3", period:str='m'):
        """
        market Factor 데이터를 계산한다

        Args:
            dfRtn (pd.DataFrame): 수익률 데이터
            dfFactor (F): 파머 프렌치 Factor 데이터
        """
        super().__init__(dfRtn, period)
        self._dfFactor = dfFactor

    def _calculate_factors(self) -> pd.DataFrame:
        dfFactor = self._calculate_rollingFactorBetas(self._dfRtn, self._dfFactor, self._period) # -> dfRtn_add_Factor_sub_Rf
        dfBetas = self._calculate_betas(dfFactor, self._period)
        print(dfBetas)
        self._dfRtn = self._rtn_merge_betas(self._dfRtn, dfBetas)  #-> dfRtn_add_dfBetas
        self._dfRtn = self._impute_missingFactorBetas(self._dfRtn)
        return self._dfRtn
        
    def _calculate_rollingFactorBetas(self, dfRtn:pd.DataFrame, dfFactor:"F-F_Research_Data_5_Factors_2x3", period:str = 'm')->pd.DataFrame:
        """
        마켓 beta를 계산한다.

        Args:
            dfRtn (pd.DataFrame):수익률 데이터
            dfFactor (F): market factor 데이터
            periods (str, optional): 데이터의 샘플 기간. Defaults to 'M'.

        Returns:
            pd.DataFrame: 시장 수익률과 시장 factor를 합하고 rf를 제외한 데이터
        """
        # factors = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']
        dfFactor_sub_Rf = dfFactor.drop('RF', axis=1)
        dfFactor_sub_Rf.index = dfFactor_sub_Rf.index.to_timestamp()
        dfFactor_sub_Rf = dfFactor_sub_Rf.resample(period).last().div(100)
        dfFactor_sub_Rf.index.name = 'Date'
        dfFactor = dfFactor_sub_Rf.join(dfRtn[f'return_1{period}']).sort_index()
        return dfFactor

    def _calculate_betas(self, dfRtnFactorSubRf:pd.DataFrame, period:str='m')->pd.DataFrame:
        """
        market beta(시장 민감도)를 계산한다.

        Args:
            dfRtnFactorSubRf (pd.DataFrame): 시장 수익률과 시장 factor를 합하고 rf를 제외한 데이터
            period (str, optional): 데이터의 샘플 기간. Defaults to 'M'.

        Returns:
            pd.DataFrame: market beta (시장 민감도)
        """
        if period is 'm' :   T = 24
        elif 'w' :   T =  24 * 52
        dfBetas = (dfRtnFactorSubRf.groupby(level='ticker',
                                    group_keys=False)
                .apply(lambda x: RollingOLS(endog=x[f"return_1{period}"],
                                            exog=sm.add_constant(x.drop(f'return_1{period}', axis=1)),
                                            window=min(T, x.shape[0]-1))
                        .fit(params_only=True)
                        .params
                        .drop('const', axis=1)))
        return dfBetas
    
    def _rtn_merge_betas(self, dfRtn:pd.DataFrame, dfBetas:pd.DataFrame) -> pd.DataFrame:
        """
        beta 와 수익률을 결합한다.
        

        Args:
            dfRtn (pd.DataFrame): 수익률
            dfBetas (pd.DataFrame): market beta (시장 민감도)

        Returns:
            pd.DataFrame: 수익률과 beta가 합해진 데이터
        """
        dfRtn =  dfRtn.join(dfBetas.groupby(level='ticker').shift())  # 1개월 수익에 맞추기 위해 쉬프트 함
        return dfRtn

    
    def _impute_missingFactorBetas(self, dfRtn_add_dfBetas:pd.DataFrame) -> pd.DataFrame:
        """
        누락된 데이터를 보정한다.
        

        Args:
            dfRtn_add_dfBetas (pd.DataFrame): 수익률과 beta가 합해진 데이터

        Returns:
            pd.DataFrame: 수익률과 beta가 합해진 데이터
        """
        factors = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']
        dfRtn_add_dfBetas.loc[:, factors] =  dfRtn_add_dfBetas.groupby('ticker')[factors].apply(lambda x: x.fillna(x.mean()))
        return dfRtn_add_dfBetas
    
    
    
# ===============================================
# momentum factors 
# ===============================================
class MomentumFactors(Factors):
    def __init__(self, dfRtn:pd.DataFrame, period:str='m'): super().__init__(dfRtn, period)

    def _calculate_factors(self)->pd.DataFrame:
        """
        모멘텀 factor를 계산한다.

        Args:
            dfRtn (pd.DataFrame): 수익률 데이터

        Returns:
            pd.DataFrame: 모멘텀 factor가 추가된 수익률 데이터
        """
        for lag in [2,3,6,9,12]: 
            self._dfRtn[f'momentum_{lag}'] = self._dfRtn[f'return_{lag}{self._period}'].sub(self._dfRtn[f"return_1{self._period}"])
        self._dfRtn[f'momentum_3_12'] = self._dfRtn[f'return_12{self._period}'].sub(self._dfRtn[f"return_3{self._period}"])
        return self._dfRtn
        
# ===============================================
# date_Indicators
# ===============================================

class DateIndicators(Factors):
    def __init__(self, dfRtn:pd.DataFrame): super().__init__(dfRtn)
        
    def _calculate_factors(self)->pd.DataFrame:
        dates = self._dfRtn.index.get_level_values('Date')
        self._dfRtn['year'] = dates.year
        self._dfRtn['month'] = dates.month
        return self._dfRtn
    
        
# ===============================================
# lagged returns
# ===============================================
class LaggedReturns(Factors):
    def __init__(self, dfRtn:pd.DataFrame, period:str='m'): super().__init__(dfRtn, period)
    
    def _calculate_factors(self)->pd.DataFrame:
        for t in range(1, 7):
            self._dfRtn[f'return_1{self._period}_t-{t}'] =  self._dfRtn.groupby(level='ticker')[f"return_1{self._period}"].shift(t)
        return self._dfRtn
        
# ===============================================
# Target: Holding Period Returns
# ===============================================

class HoldingPeriodReturns(Factors):
    def __init__(self, dfRtn:pd.DataFrame, period:str='m'): super().__init__(dfRtn, period)

    def _calculate_factors(self)->pd.DataFrame:
        for t in [1,2,3,6,12]:
            self._dfRtn[f'target_{t}{self._period}'] = self._dfRtn.groupby(level='ticker')[f'return_{t}{self._period}'].shift(-t)
        return self._dfRtn
    
# ===============================================
# dynamic_size_factors
# ===============================================

class DynamicSizeFactors(Factors):
    def __init__(self, dfRtn:pd.DataFrame, dfPrices:pd.DataFrame, dfProfiles:pd.DataFrame): 
        super().__init__(dfRtn)
        self._dfPrices = dfPrices
        self._dfProfiles= dfProfiles
        
        
    def _calculate_factors(self)->pd.DataFrame:
        msize = self._calculate_msize(self._dfPrices, self._dfRtn, self._dfProfiles)
        self._dfRtn['msize'] = (msize.apply(lambda x: pd.qcut(x, q=10, labels=list(range(1, 11)))
                            .astype(int), axis=1)
                    .stack()
                    .swaplevel())
        self._dfRtn['msize']  = self._dfRtn['msize'].fillna(-1)
        return self._dfRtn


    def _calculate_msize(self, dfPrices:pd.DataFrame, dfRtn:pd.DataFrame, dfProfiles:pd.DataFrame) -> pd.DataFrame:
        sizeFactor = self._calculate_sizeFactors(dfPrices, dfRtn)
        msize = (sizeFactor
            .mul(dfProfiles
                .loc[sizeFactor.columns, 'enterpriseValue'])).dropna(axis=1, how='all')
        return msize

    def _calculate_sizeFactors(self, dfPrices:pd.DataFrame, dfRtn:pd.DataFrame):
        sizeFactor = (dfPrices
                        .reindex(index=dfRtn.index.get_level_values('Date').unique(), columns= dfRtn.index.get_level_values('ticker').unique())
                        .sort_index(ascending=False)
                        .pct_change()
                        .fillna(0)
                        .add(1)
                        .cumprod())
        return sizeFactor
# ===============================================
# sector_factor
# ===============================================

class SectorFactors(Factors):
    def __init__(self, dfRtn:pd.DataFrame, profile:pd.Series): 
        super().__init__(dfRtn)
        self._profile= profile
        
    def _calculate_factors(self) -> pd.DataFrame:
        sector = pd.DataFrame(self._profile['sector'])
        sector.index.name='ticker'
        self._dfRtn= self._dfRtn.join(sector)
        self._dfRtn.sector = self._dfRtn.sector.fillna('Unknown')
        return self._dfRtn
    

# ===============================================
# create_dummy_data
# ===============================================

class DummyVariables(Factors):
    def __init__(self, dfRtn:pd.DataFrame): 
        super().__init__(dfRtn)
   
    def _calculate_factors(self) -> pd.DataFrame:
        self._dfRtn = pd.get_dummies(self._dfRtn,
                                    columns=['year','month', 'msize', 'sector'],
                                    prefix=['year','month', 'msize',''],
                                    prefix_sep=['_', '_', '_', ''])
        self._dfRtn = self._dfRtn.rename(columns={c:c.replace('.0', '') for c in self._dfRtn.columns})
        return self._dfRtn