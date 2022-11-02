from typing import List, Iterator
import asyncio
import pandas as pd
import yfinance as yf
import asyncio



class Profiles:

    @staticmethod   
    def load_info_snp500() -> pd.DataFrame:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        df = pd.read_html(url, header=0)[0]
        df.columns = ['ticker', 'name', 'sec_filings', 'gics_sector', 'gics_sub_industry', 'location', 'first_added', 'cik', 'founded']
        df = df.drop('sec_filings', axis=1).set_index('ticker')
        return df
    
    @staticmethod   
    def generate_by(symbols:List[str])-> Iterator[pd.Series]:
            for symbol in symbols:
                    ds = pd.Series({profile:yf.Ticker(symbol).info.get(profile) for profile in ['longName', 'industry', 'sector' ,'enterpriseValue']})
                    ds.name=symbol
                    yield ds
                    
    @staticmethod      
    async   def generate_by(symbols:List[str])-> Iterator[pd.Series]:
                for symbol in symbols:
                        ds = pd.Series({profile:yf.Ticker(symbol).info.get(profile) for profile in ['longName', 'industry', 'sector' ,'enterpriseValue']})
                        ds.name=symbol
                        yield ds
                        asyncio.sleep(1)
                        
                        
                        
# class ProfilesYf:
#         @staticmethod
#         async   def _load_profiles(symbols:List[str])-> Iterator[pd.Series]:
#                 def get_profile_list(): yield from ['longName', 'industry', 'sector' ,'enterpriseValue']
#                 for symbol in symbols:
#                     try:
#                         ds = pd.Series({profile:yf.Ticker(symbol).info.get(profile) for profile in get_profile_list()})
#                         ds.name=symbol
#                         yield ds
#                         await   asyncio.sleep(1)
#                     except Exception as e:
#                         print(e)
#                         pass
                
#         @staticmethod
#         def load_nasdaq()-> Iterator[pd.Series]:
#             """
#             나스닥 종목에 대한 프로파일을 제공한다.

#             Returns: 이터레이션을 리턴 한다.
                
#             Yields:
#                 Iterator[pd.Series]: 프로파일을 이터레이션한다.
                
#             from provider.profiles.profiles import Profiles    
#             Profiles.load_nasdaq()
            
#             >>  
#             ongName                     Apple Inc.
#             industry           Consumer Electronics
#             sector                       Technology
#             enterpriseValue           2561706295296
#             Name: AAPL, dtype: object
            
#             longName             Microsoft Corporation
#             industry           Software—Infrastructure
#             sector                          Technology
#             enterpriseValue              1728178552832
#             Name: MSFT, dtype: object
#             """
#             return ProfilesYf._load_profiles(Symbols.load_nasdaq)

#         @staticmethod
#         def load_snp500()-> Iterator[pd.Series]:
#             """
#             SNP500 종목에 대한 프로파일을 제공한다.

#             Returns: 이터레이션을 리턴 한다.
                
#             Yields:
#                 Iterator[pd.Series]: 프로파일을 이터레이션한다.
                
#             from provider.profiles.profiles import Profiles    
#             Profiles.load_snp500()
            
#             >>  
#             longName              3M Company
#             industry           Conglomerates
#             sector               Industrials
#             enterpriseValue      83095257088
#             Name: MMM, dtype: object
            
#             longName                  A. O. Smith Corporation
#             industry           Specialty Industrial Machinery
#             sector                                Industrials
#             enterpriseValue                        8211533312
#             Name: AOS, dtype: object
#             """
            
#             return ProfilesYf._load_profiles(Symbols.load_snp500)