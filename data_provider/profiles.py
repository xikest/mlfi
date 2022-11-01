from typing import List, Iterator
import asyncio
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr  # pip install -U finance-datareader

from .symbols import Symbols

class Profiles:
        @staticmethod
        def _load_profiles(symbols:Iterator[str])-> Iterator[pd.Series]:
            def _get_profile_list(): yield from ['longName', 'industry', 'sector' ,'enterpriseValue']
            for symbol in symbols():
                    ds = pd.Series({profile:yf.Ticker(symbol).info.get(profile) for profile in _get_profile_list()})
                    ds.name=symbol
                    yield ds
                
        @staticmethod
        def load_nasdaq()-> Iterator[pd.Series]:
            """
            나스닥 종목에 대한 프로파일을 제공한다.

            Returns: 이터레이션을 리턴 한다.
                
            Yields:
                Iterator[pd.Series]: 프로파일을 이터레이션한다.
                
            from provider.profiles.profiles import Profiles    
            Profiles.load_nasdaq()
            
            >>  
            ongName                     Apple Inc.
            industry           Consumer Electronics
            sector                       Technology
            enterpriseValue           2561706295296
            Name: AAPL, dtype: object
            
            longName             Microsoft Corporation
            industry           Software—Infrastructure
            sector                          Technology
            enterpriseValue              1728178552832
            Name: MSFT, dtype: object
            """
            return Profiles._load_profiles(Symbols.load_nasdaq)

        @staticmethod
        def load_snp500()-> Iterator[pd.Series]:
            """
            SNP500 종목에 대한 프로파일을 제공한다.

            Returns: 이터레이션을 리턴 한다.
                
            Yields:
                Iterator[pd.Series]: 프로파일을 이터레이션한다.
                
            from provider.profiles.profiles import Profiles    
            Profiles.load_snp500()
            
            >>  
            longName              3M Company
            industry           Conglomerates
            sector               Industrials
            enterpriseValue      83095257088
            Name: MMM, dtype: object
            
            longName                  A. O. Smith Corporation
            industry           Specialty Industrial Machinery
            sector                                Industrials
            enterpriseValue                        8211533312
            Name: AOS, dtype: object
            """
            
            return Profiles._load_profiles(Symbols.load_snp500)

    
