from typing import Union, Optional, Iterator
from dataclasses import dataclass
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
import pandas_datareader.data as web
# import asyncio

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from tqdm import tqdm


@dataclass(unsafe_hash =True)
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
    data_src:Optional[str] = None
    market_factors:Optional[str] = None
    enable_profile_engineering:bool = True
        

class Profiles:
    
    def _init__(self):
        pass

    @staticmethod 
    def load_profiles(label:str='snp500') -> Iterator[Profile]:
        """ 입력된 시장에 맞는 정보를 제공한다.

        Args:
            market (str, optional): 시장 정보. Defaults to 'snp500'.

        Yields:
            Generator[Info]: ticker 별로 정보를 생성한다(generator)
        """

        if label == 'snp500':
            yield from Profiles()._load_profile_snp500(data_src = 'yahoo', market_factors = 'F-F_Research_Data_5_Factors_2x3')
            
        elif label == 'kospi':
            yield from Profiles()._load_profile_stocks_from_fdr('KOSPI', data_src = 'naver')
            
        elif label == 'nasdaq':
            yield from Profiles()._load_profile_stocks_from_fdr('NASDAQ', data_src = 'yahoo', market_factors = 'F-F_Research_Data_5_Factors_2x3')

        elif label == 'etf_us':
            url = 'https://kr.investing.com/etfs/usa-etfs' # 인베스팅 닷컴 ETF 미국 ETF 리스트
            yield from Profiles()._load_profile_ETF_from_investing(url, data_src = 'yahoo', market_factors = 'F-F_Research_Data_5_Factors_2x3')
            
        elif label == 'etf_kr':
            print('etf_kr')
            url = 'https://kr.investing.com/etfs/south-korea-etfs' # 인베스팅 닷컴 ETF 한국 ETF 리스트
            yield from Profiles()._load_profile_ETF_from_investing(url, data_src = 'naver')
            





    def _load_profile_snp500(self, data_src:str, market_factors:str = 'F-F_Research_Data_5_Factors_2x3') -> Iterator[Profile]:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        df = pd.read_html(url, header=0)[0]
        df.loc[:,'Symbol'] = df.loc[:, 'Symbol'].map(lambda x:x.replace('.' , '-'))
        # df = df.iloc[:3,:]
        
        yield from{Profile(ticker=info['Symbol'],
                            name=info['Security'],
                            sub_industry = info['GICS Sub-Industry'],
                            location=info['Headquarters Location'],
                            first_added=info['Date first added'],
                            cik=info['CIK'],
                            founded=info['Founded'],
                            sector=yf.Ticker(info['Symbol']).info.get('sector'),
                            enterpriseValue = yf.Ticker(info['Symbol']).info.get('enterpriseValue'),
                            data_src = data_src,
                            market_factors = market_factors,
                            enable_profile_engineering = True) for _, info in df.iterrows()}

    def _load_profile_ETF_from_investing(self, url:str, data_src:str, market_factors:Optional[str]=None)-> Iterator[Profile]:
        profiles = set()
        
        hdr ={'User-Agent': 'Mozilla/5.0'}
        req = Request(url,headers=hdr)
        page = urlopen(req)
        pages_etf = BeautifulSoup(page,'html.parser').select_one('#etf_issuer > select')
        issuers ={page['value']:page.string for page in pages_etf.select('option')}

        for issuer in tqdm(issuers.keys()):
            req = Request(f'{url}?&issuer_filter={issuer}',headers=hdr) 
            page = urlopen(req)
            soup = BeautifulSoup(page,'html.parser')
            pages_ETF = soup.select_one('#etfs > tbody')
                        
            for page_ETF in pages_ETF:
                profiles.add(Profile(ticker=page_ETF.find("td", {'class':'left symbol'})['title'],
                                                            name=page_ETF.find("span", {'class':'alertBellGrayPlus js-plus-icon genToolTip oneliner'})['data-name'],
                                                            data_src = data_src,
                                                            market_factors=market_factors,
                                                            enable_profile_engineering =False))
        yield from profiles
                            
    def _load_profile_stocks_from_fdr(self, market:str = 'S&P500', data_src:str='fdr', market_factors:Optional[str]=None) -> Iterator[Profile]:
        """
        market = S&P500 , NASDAQ, KOSPI, KOSDAQ
        """
        info = fdr.StockListing(market).rename(columns={'Code':'Symbol'})
        
        profiles = {Profile(ticker=info['Symbol'],
                            name=info['Name'],
                            data_src = data_src,
                            market_factors=market_factors,
                            enable_profile_engineering = False) for _, info in info.iterrows()}  #중복 제거
        
        
        yield from profiles
