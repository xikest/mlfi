from data_provider.data import Data
from data_provider.functions_for_data.functions_path import FunctionPath

    
    
from data_provider import Data
# snp500 = Data("snp500")
nasdaq = Data("nasdaq")
# kospi = Data("kospi")
# etf_us = Data(market="etf_us")
# etf_kr = Data("etf_kr")

print('finish')
FunctionPath.HDFS.save_to_HDFS(pd.DataFrame(['dummy']), 'df_dummy','df_dummy.h5')
