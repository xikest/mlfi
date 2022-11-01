from typing import List, Iterator
import asyncio
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr  # pip install -U finance-datareader



class Symbols: 
    
    @staticmethod
    def _load_symbols(MARKET:str) -> Iterator[str]:
        yield from list(fdr.StockListing(MARKET).loc[:,'Symbol'].drop_duplicates().reset_index(drop=True).values)
            
    @staticmethod
    def load_nasdaq() -> Iterator[str]: 
        """
        나스닥의 symbols 리스트를 불러온다

        Returns:
            Iterator[str]: 나스닥 symbols 이터레이터
            
        from provider.symbols import Symbols    
        Symbols.load_nasdaq()    
        >>  AAPL
            MSFT
            AMZN
            TSLA
            GOOGL
            GOOG
            NVDA
            PEP
            META
            COST
            AVGO
            ASML
            CSCO
            AZN
        """ 
        return Symbols._load_symbols('NASDAQ')
        
    @staticmethod
    def load_snp500() ->  Iterator[str]: 
        """
        Snp500 symbols 리스트를 불러온다

        Returns:
            Iterator[str]: Snp500 symbols 이터레이터
            
        from provider.symbols import Symbols    
        Symbols.load_snp500()   
        >>  MMM
            AOS
            ABT
            ABBV
            ABMD
            ACN
            ATVI
            ADM
            ADBE
            ADP
            AAP
            AES
            AFL
            A
            APD
        """ 
        return Symbols._load_symbols('SP500')