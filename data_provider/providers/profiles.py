from typing import Union, Optional, Iterator
from dataclasses import dataclass
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
# import asyncio

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from tqdm import tqdm


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
            
        elif 'kospi':
            yield from Profiles()._load_profile_stocks_from_fdr('KOSPI')
            
        elif 'nasdaq':
            yield from Profiles()._load_profile_stocks_from_fdr('NADAQ')

        elif 'etf_us':
            url = 'https://kr.investing.com/etfs/usa-etfs' # 인베스팅 닷컴 ETF 미국 ETF 리스트
            yield from Profiles()._load_profile_ETF_from_investing(url)
        elif 'etf_kr':
            url = 'https://kr.investing.com/etfs/south-korea-etfs' # 인베스팅 닷컴 ETF 한국 ETF 리스트
            yield from Profiles()._load_profile_ETF_from_investing(url)
        

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


    def _load_profile_ETF_from_investing(self, url)-> Iterator[Profile]:
        req = Request(url,headers={'User-Agent': 'Mozilla/5.0'})
        page = urlopen(req)
        pages_etf = BeautifulSoup(page).select_one('#etf_issuer > select')
        issuers ={page['value']:page.string for page in pages_etf.select('option')}

        for issuer in tqdm(issuers.keys()):
            req = Request(f'{site}?&issuer_filter={issuer}',headers=hdr) 
            page = urlopen(req)
            soup = BeautifulSoup(page)
            pages_ETF = soup.select_one('#etfs > tbody')
            
            for page_ETF in pages_ETF:
                yield Profile(ticker=page_ETF.find("td", {'class':'left symbol'})['title'],
                                name=page_ETF.find("span", {'class':'alertBellGrayPlus js-plus-icon genToolTip oneliner'})['data-name'])
                
    def _load_profile_stocks_from_fdr(self, market) -> Iterator[Profile]:
        info = fdr.StockListing('KOSPI')
        for _, info in info.iterrows():
                yield Profile(ticker=info['Code'],
                                name=info['Name'],
                                enterpriseValue =info['Marcap']
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