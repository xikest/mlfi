from typing import Union, Optional, Iterator
import pandas as pd
import yfinance as yf
# import asyncio
from dataclasses import dataclass

@dataclass
class Profile:
    ticker:Optional[str] = None
    name:Optional[str] = None
    sector:Optional[str] = None
    sub_industry:Optional[str] = None
    location:Optional[str] = None
    first_added:Optional[str] = None
    cik:Optional[str] = None
    founded:Optional[str] = None
    enterpriseValue:Union[int, float]=None
        

class Profiles:
    
    def _init__(self):
        pass

    @staticmethod 
    
    def load_profiles(market:str='snp500') -> Iterator[Profile]:
        """ 입력된 시장에 맞는 정보를 제공한다.

        Args:
            market (str, optional): 시장 정보. Defaults to 'snp500'.

        Yields:
            Generator[Info]: ticker 별로 정보를 생성한다(generator)
        """

        if market == 'snp500':
            yield from Profiles()._load_profile_snp500()
        

    def _load_profile_snp500(self) -> Iterator[Profile]:
            url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            df = pd.read_html(url, header=0)[0]
            df.loc[:,'Symbol'] = df.loc[:, 'Symbol'].map(lambda x:x.replace('.' , '-'))
            df = df.iloc[:3,:]
            for _, info in df.iterrows():
                yield Profile(ticker=info['Symbol'],
                                        name=info['Security'],
                                        sub_industry = info['GICS Sub-Industry'],
                                        location=info['Headquarters Location'],
                                        first_added=info['Date first added'],
                                        cik=info['CIK'],
                                        founded=info['Founded'],
                                        sector=yf.Ticker(info['Symbol']).info.get('sector'),
                                        enterpriseValue = yf.Ticker(info['Symbol']).info.get('enterpriseValue')
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