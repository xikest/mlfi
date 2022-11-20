
import pandas as pd
# import seaborn as sns

class Returns:
    
    def __init__(self, dfPrices:pd.DataFrame, period:str ='m'):
        self._data = None
        self._dfPrices = dfPrices
        self._period = period
        
    def get_data(self):
        if self._data is None: self._data = self._drop_less_than_periods(self._calculate_rtn(self._dfPrices, self._period), self._period)
        return self._data
    
    def _calculate_rtn(self, dfPrices:pd.DataFrame,  period:str) -> pd.DataFrame:
        outlier_cutoff = 0.01
        data = pd.DataFrame()
        lags = [1, 2, 3, 6, 9, 12]
        
        dfPrices = dfPrices.resample(period).last() 
        dfPrices.columns.name='ticker'
        for lag in lags:
            data[f'return_{lag}{period}'] = (dfPrices
                                    .pct_change(lag)
                                    .stack()
                                    .pipe(lambda x: x.clip(lower=x.quantile(outlier_cutoff),
                                                            upper=x.quantile(1-outlier_cutoff)))
                                    .add(1)
                                    .pow(1/lag)
                                    .sub(1)
                                    )
        return data.swaplevel().dropna()

    def _drop_less_than_periods(self, data:pd.DataFrame, period:str='m'):
        if period == 'm' :    min_obs = 120
        elif period == 'w' :   min_obs = 120 * 52
        idx = pd.IndexSlice
        nobs = data.stack().groupby(level='ticker').size()
        keep = nobs[nobs>min_obs].index
        return data.loc[idx[keep,:], :]

    # def plot_correlaton(self) -> pd.DataFrame:
    #     """
    #     데이터의 correlation을 계산하여 clustermap 그래프를 보여준다.

    #     Args:
    #         df_rtn (pd.DataFrame): 수익률 데이터 

    #     Returns: None
    #     """
    #     sns.clustermap(self.get_data.corr('spearman'), annot=True, center=0, cmap='Blues');
    #     return None