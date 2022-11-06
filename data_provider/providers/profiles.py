from typing import List, Iterator
import pandas as pd
import yfinance as yf
# import asyncio
from dataclasses import dataclass

@dataclass
class Info:
    ticker:str = None
    name:str=None
    gics_sector:str=None
    gics_sub_industry:str=None
    location:str=None
    first_added:str=None
    cik:str=None
    founded:str=None
    profiles:pd.DataFrame=None
        
        


class Profiles:

    @staticmethod 
    
    def load_info(market:str='snp500') -> Iterator[Info]:
        """ 입력된 시장에 맞는 정보를 제공한다.

        Args:
            market (str, optional): 시장 정보. Defaults to 'snp500'.

        Yields:
            Generator[Info]: ticker 별로 정보를 생성한다(generator)
        """

        if market == 'snp500':
            yield from Profiles.load_info_snp500()
        
    @staticmethod 
    def _load_info_snp500() -> Iterator[Info]:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        df = pd.read_html(url, header=0)[0]
        df.loc[:,'Symbol'] = df.loc[:, 'Symbol'].map(lambda x:x.replace('.' , '-'))
        
        for _, info in df.iterrows():
           yield Info(ticker=info['Symbol'],
                                name=info['Security'],
                                gics_sector=info['GICS Sector'],
                                gics_sub_industry = info['GICS Sub-Industry'],
                                location=info['Headquarters Location'],
                                first_added=info['Date first added'],
                                cik=info['CIK'],
                                founded=info['Founded'],
                                profiles=pd.Series({profile:yf.Ticker(info['Symbol']).info.get(profile) for profile in ['longName', 'industry', 'sector' ,'enterpriseValue']})
           )
            
         
         
            
        # yield from [Info(ticker=info['Symbol'],
        #                     name=info['Security'],
        #                     gics_sector=info['GICS Sector'],
        #                     gics_sub_industry = info['GICS Sub-Industry'],
        #                     location=info['Headquarters Location'],
        #                     first_added=info['Date first added'],
        #                     cik=info['CIK'],
        #                     founded=info['Founded'],
        #                     ) for _, info in df.iterrows()]

    
    # @staticmethod   
    # def generate_by(symbols:List[str])-> Generator[pd.Series]:
    #         for symbol in symbols:
    #             try:
    #                 ds = pd.Series({profile:yf.Ticker(symbol).info.get(profile) for profile in ['longName', 'industry', 'sector' ,'enterpriseValue']})
    #                 ds.name=symbol
    #                 yield ds
    #             except Exception as e:
    #                 print(f'{symbol},generate_by: {e}')
    #                 pass
                    
    # @staticmethod      
    # async   def generate_by(symbols:List[str])-> Iterator[pd.Series]:
        
    #             async   def gen_by(symbols):
    #                         for symbol in symbols:
    #                             try:
    #                                 ds = pd.Series({profile:yf.Ticker(symbol).info.get(profile) for profile in ['longName', 'industry', 'sector' ,'enterpriseValue']})
    #                                 ds.name=symbol
    #                                 yield ds
    #                                 asyncio.sleep(1)
    #                             except Exception as e:
    #                                 print(f'{symbol},generate_by: {e}')
    #                                 pass
    #             return asyncio.gather(gen_by(symbols))
    
    
    
    
    
    
                        
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