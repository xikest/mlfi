
import pandas as pd

from .function_for_engineering.returns import Returns
from .function_for_engineering.factors import MarketFactors, MomentumFactors, DateIndicators, LaggedReturns, HoldingPeriodReturns, DynamicSizeFactors, SectorFactors, DummyVariables


class DataEngineer:
    """ 데이터를 가공한다.
    """
    def __init__(self, dfPrices:pd.DataFrame, dfFactors:pd.DataFrame, dfprofiles:pd.DataFrame, period='m'):
        """_summary_

        Args:
            dfPrices (pd.DataFrame): 계산한 가격 데이터이다. 인덱스는 멀티 인덱스이고, index(Date, Symbol), columns[price]
            dfFactors (F): 파머 프렌치 팩터 데이터이다. F-F_Research_Data_5_Factors_2x3
        """
        self._data = None
        self._dfPrices = dfPrices
        self._dfFactors = dfFactors
        self._dfprofiles = dfprofiles
        self._period = period
        pass
    
    def get_data(self):
        if self._data is None:  self._data = self.engineering_data(self._dfPrices, self._dfprofiles, self._dfFactors, self._period)
        return self._data
        
        
    def engineering_data(self, dfPrices:pd.DataFrame, dfprofiles:pd.DataFrame, dfFactors:pd.DataFrame, period:str):
        dfRtn = Returns(dfPrices, period).get_data()
        dfRtn = MarketFactors(dfRtn, dfFactors, period).get_data()
        dfRtn = MomentumFactors(dfRtn, period).get_data()
        dfRtn = DateIndicators(dfRtn, period).get_data()
        dfRtn = LaggedReturns(dfRtn, period).get_data()
        dfRtn = HoldingPeriodReturns(dfRtn, period).get_data()
        dfRtn = DynamicSizeFactors(dfRtn,dfPrices, dfprofiles).get_data()  
        dfRtn = SectorFactors(dfRtn, dfprofiles).get_data()  
        dfRtn = DummyVariables(dfRtn).get_data() 
        return dfRtn 
