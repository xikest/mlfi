from data_provider import BasicData, CustomizingData
from data_provider.functions_for_data.functions_path import FunctionPath
import pandas as pd
    
    
# from data_provider import Data
# # snp500 = BasicData("snp500")
# # nasdaq = BasicData("nasdaq")
# # kospi = BasicData("kospi")
# etf_us = BasicData(label="etf_us")
# # # etf_kr = BasicData("etf_kr")

# # print('finish')
# # FunctionPath.HDFS.save_to_HDFS(pd.DataFrame(['dummy']), 'df_dummy','df_dummy')



# # CustomData

selected_pf = ['XLV', 'XLB', 'XLP','XLF', 'XLI', 'XLC', 'XLK', 'XLU', 'XLY','XLE']

# sectors = CustomizingData(etf_us).make_dataset(selected_pf)

# print(sectors)


# selected_pf =  ['VNQ'] #리츠
# selected_pf =  ['TLT'] # 채권
# selected_pf =  ['GLD'] #금

cd = CustomizingData('etf_us')
sectors = cd.make_dataset(selected_pf , dataset_label='sectors')
print(sectors)