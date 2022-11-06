
from numpy.linalg import inv
import pandas as pd

## 켈리 룰에 따라 포트폴리오 비중을 계산
## 기본 주기는 Monthly이고, 입력데이터의 시작 간에 따라 비중에 달라진다.
## df_rtn: 입력하는 return, 인덱스는 datetime으로 되어 있어야 한다.
## pf_return_by_kelly: 켈리 룰에 따라 계산한 PF의 수익
## kelly_alloc_weight: 켈리 룰에 따른 포트 폴리오 비중

class KellyAloc:
  def __init__(self, dfPrices:pd.DataFrame, period='M'):
    dfRtn = dfPrices.resample(period).last().pct_change().dropna(how='all').dropna(axis=1)
    self._pfRtn_by_kelly, self._weight_by_kelly= calculate_pfKellyReturn_by(dfRtn)
    print('kelly in here')
    
  def get_rtn(self):
    return self._pfRtn_by_kelly
  
  def get_weight(self):
    return self._weight_by_kelly


def calculate_pfKellyReturn_by(dfRtn:pd.DataFrame):
  weight_kelly = calculate_kelly_weight(dfRtn)
  return dfRtn.mul(weight_kelly).sum(axis=1).to_frame('Kelly').add(1).cumprod().sub(1), weight_kelly

def calculate_kelly_weight(df_rtn):
  kelly_alloc = calculate_kelly_allocation(df_rtn)
  return kelly_alloc.div(kelly_alloc.sum())
    
def calculate_kelly_allocation(df_rtn):
  stocks = df_rtn.columns
  cov = df_rtn.cov()
  precision_matrix = pd.DataFrame(inv(cov), index=stocks, columns=stocks)
  return df_rtn.mean().dot(precision_matrix)