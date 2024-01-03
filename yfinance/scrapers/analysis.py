import io
import pandas as pd
import json
import numpy as np

from yfinance import utils
from yfinance.data import YfData


class Analysis:

    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy

        self._quarterly_eps_history = pd.DataFrame()

        self._quarterly_eps_trend = pd.DataFrame()
        self._yearly_eps_trend = pd.DataFrame()

        self._quarterly_rev_est = pd.DataFrame()
        self._yearly_rev_est = pd.DataFrame()

        self._quarterly_eps_est = pd.DataFrame()
        self._yearly_eps_est = pd.DataFrame()
        
        self._already_scraped = False

    @property
    def quarterly_eps_history(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._quarterly_eps_history

    @property
    def quarterly_eps_trend(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._quarterly_eps_trend
    
    @property
    def yearly_eps_trend(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._yearly_eps_trend

    @property
    def quarterly_rev_est(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._quarterly_rev_est
    
    @property
    def yearly_rev_est(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._yearly_rev_est

    @property
    def quarterly_eps_est(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._quarterly_eps_est
    
    @property
    def yearly_eps_est(self) -> pd.DataFrame:
        if not self._already_scraped:
            self._scrape(self.proxy)
        return self._yearly_eps_est

    def _scrape(self, proxy):
        url = f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{self._symbol}?modules=earningsHistory,earningsTrend'

        json_str = self._data.cache_get(url=url, proxy=proxy).text
        json_data = json.loads(json_str)
        data_raw = json_data["quoteSummary"]["result"]

        if data_raw is not None:
            data_raw = data_raw[0]

            index = ('epsActual', 'epsEstimate', 'epsDifference', 'surprisePercent')
            columns = {pd.Timestamp(d['quarter']['raw'], unit='s'): {i: d[i].get('raw') for i in index} for d in data_raw['earningsHistory']['history'] if d['quarter'].get('raw') is not None}
            self._quarterly_eps_history = pd.DataFrame(data=columns, index=index).replace(np.nan, None)

            index = ('current', '7daysAgo', '30daysAgo', '60daysAgo', '90daysAgo')
            columns = {pd.Timestamp(d['endDate']): {i: d['epsTrend'][i].get('raw') for i in index} for d in data_raw['earningsTrend']['trend'] if d['period'] in ('0q', '+1q') and d.get('endDate') is not None}
            self._quarterly_eps_trend = pd.DataFrame(data=columns, index=index).replace(np.nan, None)
            columns = {pd.Timestamp(d['endDate']): {i: d['epsTrend'][i].get('raw') for i in index} for d in data_raw['earningsTrend']['trend'] if d['period'] in ('0y', '+1y') and d.get('endDate') is not None}
            self._yearly_eps_trend = pd.DataFrame(data=columns, index=index).replace(np.nan, None)

            index = ('avg', 'low', 'high', 'yearAgoEps', 'numberOfAnalysts', 'growth')
            columns = {pd.Timestamp(d['endDate']): {i: d['earningsEstimate'][i].get('raw') for i in index} for d in data_raw['earningsTrend']['trend'] if d['period'] in ('0q', '+1q') and d.get('endDate') is not None}
            self._quarterly_eps_est = pd.DataFrame(data=columns, index=index).replace(np.nan, None)
            columns = {pd.Timestamp(d['endDate']): {i: d['earningsEstimate'][i].get('raw') for i in index} for d in data_raw['earningsTrend']['trend'] if d['period'] in ('0y', '+1y') and d.get('endDate') is not None}
            self._yearly_eps_est = pd.DataFrame(data=columns, index=index).replace(np.nan, None)

            index = ('avg', 'low', 'high', 'numberOfAnalysts', 'yearAgoRevenue', 'growth')
            columns = {pd.Timestamp(d['endDate']): {i: d['revenueEstimate'][i].get('raw') for i in index} for d in data_raw['earningsTrend']['trend'] if d['period'] in ('0q', '+1q') and d.get('endDate') is not None}
            self._quarterly_rev_est = pd.DataFrame(data=columns, index=index).replace(np.nan, None)
            columns = {pd.Timestamp(d['endDate']): {i: d['revenueEstimate'][i].get('raw') for i in index} for d in data_raw['earningsTrend']['trend'] if d['period'] in ('0y', '+1y') and d.get('endDate') is not None}
            self._yearly_rev_est = pd.DataFrame(data=columns, index=index).replace(np.nan, None)

        self._already_scraped = True