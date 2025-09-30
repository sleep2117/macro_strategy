"""
Global indices/sectors/factors universe - data collection utilities only
"""

# Note: file moved under global_universe/ for project organization

import yfinance as yf
import requests as _requests
from requests.adapters import HTTPAdapter as _HTTPAdapter
try:
    from urllib3.util.retry import Retry as _Retry
except Exception:
    _Retry = None
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo as _ZoneInfo
import warnings
warnings.filterwarnings('ignore')

# Shared HTTP session for yfinance with retry/backoff and browser-like headers
def _build_http_session():
    s = _requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    if _Retry is not None:
        retry = _Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["HEAD", "GET"]),
        )
        adapter = _HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    return s

_YF_SESSION = _build_http_session()

investment_universe = {
    'Commodities': {
        'currency': 'USD',
        'sectors': {
            'Broad':            {'index': None, 'etf': 'PDBC', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Energy_Broad':     {'index': None, 'etf': 'DBE',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Crude_Oil':        {'index': None, 'etf': 'OILK', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Natural_Gas':      {'index': None, 'etf': 'UNG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Gold':             {'index': None, 'etf': 'GLD',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Silver':           {'index': None, 'etf': 'SLV',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Palladium':        {'index': None, 'etf': 'PALL', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Platinum':         {'index': None, 'etf': 'PPLT', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Copper':           {'index': None, 'etf': 'CPER', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Industrial_Metals':{'index': None, 'etf': 'BCIM', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Agriculture_Broad':{'index': None, 'etf': 'TILL', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Dry_Bulk_Freight': {'index': None, 'etf': 'BDRY', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Carbon_Global':    {'index': None, 'etf': 'KRBN', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Carbon_California':{'index': None, 'etf': 'KCCA', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Volatility':       {'index': None, 'etf': 'VXX',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },
    'Global': {
        'currency': 'USD',
        'sectors': {
            # Global sector ETFs (USD)
            'Technology':    {'index': None, 'etf': 'IXN',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Healthcare':    {'index': None, 'etf': 'IXJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Financials':    {'index': None, 'etf': 'IXG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Cons_Discr.':   {'index': None, 'etf': 'RXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Cons_Staples':  {'index': None, 'etf': 'KXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Industrials':   {'index': None, 'etf': 'EXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Energy':        {'index': None, 'etf': 'IXC',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Materials':     {'index': None, 'etf': 'MXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Utilities':     {'index': None, 'etf': 'JXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Comm_Services': {'index': None, 'etf': 'IXP',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Defense':       {'index': None, 'etf': 'SHLD', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':   {'index': None, 'etf': 'REET', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Airlines':      {'index': None, 'etf': 'JETS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Global_BM':       {'index': None, 'etf': 'ACWI', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Global_Treasury': {'index': None, 'etf': 'IGOV', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Developed_World': {'index': None, 'etf': 'URTH', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Dividend_Growth_Global': {'index': None, 'etf': 'FID',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'High_Dividend_Global':   {'index': None, 'etf': 'IDOG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Momentum_Developed':     {'index': None, 'etf': 'IMTM',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Quality_Global':         {'index': None, 'etf': 'IQLT',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'IPO_Global':             {'index': None, 'etf': 'IPOS',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Moat_Global':            {'index': None, 'etf': 'MOTG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'CashFlow_Global':        {'index': None, 'etf': 'GCOW',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Min_Vol_Global':         {'index': None, 'etf': 'EFAV',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        }
    },
    'US': {
        'currency': 'USD',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'SPTM',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VTI','ITOT']},
            'Technology':    {'index': None, 'etf': 'XLK',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VGT','FTEC']},
            'Semiconductors':{'index': None, 'etf': 'SOXX', 'currency': 'USD', 'valuation_data': True,  'alternatives': ['SMH']},
            'Software':      {'index': None, 'etf': 'IGV',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Biotech':       {'index': None, 'etf': 'IBB',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Healthcare':    {'index': None, 'etf': 'XLV',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VHT','FHLC']},
            'Financials':    {'index': None, 'etf': 'XLF',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VFH']},
            'Cons_Discr.':   {'index': None, 'etf': 'XLY',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VCR']},
            'Cons_Staples':  {'index': None, 'etf': 'XLP',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VDC']},
            'Industrials':   {'index': None, 'etf': 'XLI',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VIS']},
            'Energy':        {'index': None, 'etf': 'XLE',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VDE']},
            'Materials':     {'index': None, 'etf': 'XLB',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VAW']},
            'Utilities':     {'index': None, 'etf': 'XLU',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['VPU']},
            'Comm_Services': {'index': None, 'etf': 'XLC',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Real_Estate':   {'index': None, 'etf': 'XLRE', 'currency': 'USD', 'valuation_data': True,  'alternatives': ['VNQ']},
            # Additional US sector/thematic ETFs
            'Defense':       {'index': None, 'etf': 'ITA',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Banks_Large':   {'index': None, 'etf': 'KBWB', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Banks_Regional':{'index': None, 'etf': 'KRE',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Insurance':     {'index': None, 'etf': 'KIE',  'currency': 'USD', 'valuation_data': True,  'alternatives': ['KBWP']},
            'Retail':        {'index': None, 'etf': 'XRT',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
        },
        'themes': {
            # Disruptive Tech
            'Battery':             {'index': None, 'etf': 'LIT',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            '5G':                  {'index': None, 'etf': 'NXTG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'AI':                  {'index': None, 'etf': 'AIQ',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'IoT':                 {'index': None, 'etf': 'SNSR',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Gaming':              {'index': None, 'etf': 'ESPO',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Robotics':            {'index': None, 'etf': 'BOTZ',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Data_Center_REITs':   {'index': None, 'etf': 'DTCR',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Blockchain':          {'index': None, 'etf': 'BLOK',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Bitcoin_Spot':        {'index': None, 'etf': 'IBIT',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Cybersecurity':       {'index': None, 'etf': 'CIBR',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Smart_Factory':       {'index': None, 'etf': 'MAKX',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Smart_Grid':          {'index': None, 'etf': 'GRID',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Quantum_Computing':   {'index': None, 'etf': 'QTUM',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Space_Exploration':   {'index': None, 'etf': 'UFO',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Autonomous_Vehicles': {'index': None, 'etf': 'DRIV',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Digital_Payments':    {'index': None, 'etf': 'IPAY',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Cloud':               {'index': None, 'etf': 'SKYY',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Exponential_Tech':    {'index': None, 'etf': 'XT',    'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            # Demographics & Culture
            'Online_Retail':       {'index': None, 'etf': 'IBUY',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Aging_Longevity':     {'index': None, 'etf': 'AGNG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Digital_Health':      {'index': None, 'etf': 'FDHT',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Luxury':              {'index': None, 'etf': 'KLXY',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Millennials':         {'index': None, 'etf': 'MILN',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Obesity_Treatment':   {'index': None, 'etf': 'THNR',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'India_Consumer':      {'index': None, 'etf': 'INCO',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Genomics':            {'index': None, 'etf': 'ARKG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Pet_Care':            {'index': None, 'etf': 'PAWZ',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            # Physical Environment
            'Hydrogen':            {'index': None, 'etf': 'HYDR',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Water_Infrastructure':{'index': None, 'etf': 'PHO',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'AgTech':              {'index': None, 'etf': 'KROP',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Uranium_Nuclear':     {'index': None, 'etf': 'NLR',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Clean_Energy':        {'index': None, 'etf': 'ICLN',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'CleanTech':           {'index': None, 'etf': 'CTEC',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Solar':               {'index': None, 'etf': 'TAN',   'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Disruptive_Materials':{'index': None, 'etf': 'DMAT',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Rare_Earths':         {'index': None, 'etf': 'REMX',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap':        {'index': '^GSPC',  'currency': 'USD', 'valuation_data': False, 'alternatives': ['SPY','VOO']},
            'Mid_Cap':          {'index': '^SP400', 'currency': 'USD', 'valuation_data': False, 'alternatives': ['IJH']},
            'Small_Cap':        {'index': '^RUT',   'currency': 'USD', 'valuation_data': False, 'alternatives': ['IWM','IJR']},
            'Small_Cap_600':    {'index': '^SP600', 'currency': 'USD', 'valuation_data': False, 'alternatives': ['IJR','SLY','VIOO']},
            'Nasdaq_100':       {'index': '^NDX',   'currency': 'USD', 'valuation_data': False, 'alternatives': ['QQQ']},
            'Micro_Cap':        {'index': None,     'etf': 'IWC',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Small_Value':      {'index': None,     'etf': 'IJS',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['VBR','RZV']},
            'Small_Growth':     {'index': None,     'etf': 'IJT',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['VBK','RZG']},
            'Mid_Value':        {'index': None,     'etf': 'IWS',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Mid_Growth':       {'index': None,     'etf': 'IWP',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Equal_Weight':     {'index': None,     'etf': 'RSP',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Low_Vol':          {'index': None,     'etf': 'USMV',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['SPLV']},
            'High_Beta':        {'index': None,     'etf': 'SPHB',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Low_Beta':         {'index': None,     'etf': 'SPLV',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['USMV']},
            'High_Dividend':    {'index': None,     'etf': 'VYM',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['HDV','SCHD','DVY']},
            'Dividend_Growth':  {'index': None,     'etf': 'VIG',      'currency': 'USD', 'valuation_data': True,  'alternatives': ['DGRO','DGRW']},
            'Value':            {'index': None,     'etf': 'IUSV',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['VLUE','VTV','IVE']},
            'Growth':           {'index': None,     'etf': 'IUSG',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['IVW','VUG']},
            'Momentum':         {'index': None,     'etf': 'MTUM',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Quality':          {'index': None,     'etf': 'QUAL',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['SPHQ']},
            'High_Quality':     {'index': None,     'etf': 'SPHQ',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['QUAL','QLTY']},
            'Profitability':    {'index': None,     'etf': 'COWZ',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Multi_Factor':     {'index': None,     'etf': 'LRGF',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            # Additional themes from provided images
            'Wide_Moat':        {'index': None,     'etf': 'MOAT',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Semiconductors':   {'index': None,     'etf': 'SOXX',     'currency': 'USD', 'valuation_data': True,  'alternatives': ['SMH']},
            'US_Power':         {'index': None,     'etf': 'ZAP',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'China_Tech_US':    {'index': None,     'etf': 'DRAG',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Defense_Tech':     {'index': None,     'etf': 'SHLD',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Manufacturing_Trad':{'index': None,    'etf': 'AIRR',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Pipelines':        {'index': None,     'etf': 'TPYP',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Dividend_Aristocrats': {'index': None, 'etf': 'NOBL',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Preferreds':       {'index': None,     'etf': 'PFFD',     'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            # Factor set from image (skip duplicates):
            'Dividend_Growth_US': {'index': None, 'etf': 'SCHD', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'High_Dividend_US':   {'index': None, 'etf': 'SPYD', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'IPO_US':             {'index': None, 'etf': 'IPO',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Buyback_US':         {'index': None, 'etf': 'PKW',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            # Additional large-cap/benchmark styles
            'Mega_Cap_US':        {'index': None, 'etf': 'XLG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Nasdaq100_EW':       {'index': None, 'etf': 'QQQE', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Dow_30':             {'index': None, 'etf': 'DIA',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Magnificent7_US':    {'index': None, 'etf': 'MAGS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            # Corporate credit
            'US_IG_Corp':       {'index': None,     'etf': 'LQD',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'US_HY_Corp':       {'index': None,     'etf': 'HYG',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            # Treasuries
            'US_Treasuries_20Y':{'index': None,     'etf': 'TLT',      'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            # Bond sleeves (country-level)
            'Agg_Bond_US':        {'index': None, 'etf': 'AGG',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'TIPS_US':            {'index': None, 'etf': 'TIP',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Convertibles_US':    {'index': None, 'etf': 'CWB',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'MBS_US':             {'index': None, 'etf': 'MBB',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Senior_Loans_US':    {'index': None, 'etf': 'SRLN', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Fallen_Angels_US':   {'index': None, 'etf': 'FALN', 'currency': 'USD', 'valuation_data': True, 'alternatives': []}
        }
    },

    'Europe': {
        'currency': 'EUR',
        'sectors': {
            # Technology: use ETF as primary (index has short history on Yahoo)
            'Technology':    {'index': None,     'etf': 'EXV3.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': ['ESIT.L']},
            'Healthcare':    {'index': 'SXDP.Z', 'etf': 'CH5.L',  'currency': 'GBp', 'valuation_data': True, 'alternatives': ['EXV4.DE']},
            'Utilities':     {'index': 'SX6P.Z', 'etf': 'XS6R.L', 'currency': 'GBp', 'valuation_data': True, 'alternatives': ['EXH9.DE']},
            'Telecom':       {'index': 'SXKP.Z', 'etf': 'EXV2.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Automobiles':   {'index': 'SXAP.Z', 'etf': 'EXV5.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Basic_Resrcs':  {'index': 'SXPP.Z', 'etf': 'EXV6.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Chemicals':     {'index': 'SX4P.Z', 'etf': 'EXV7.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            # Banks: use ETF as primary (index has short history on Yahoo)
            'Banks':         {'index': None,     'etf': 'BNKE.L', 'currency': 'GBP', 'valuation_data': True, 'alternatives': ['EXV1.DE']},
            'Insurance':     {'index': 'SXIP.Z', 'etf': 'EXH5.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Industrials':   {'index': 'SXNP.Z', 'etf': 'EXH4.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': ['ESIN.L']},
            'Construction':  {'index': 'SXOP.Z', 'etf': 'EXV8.DE', 'currency': 'EUR', 'valuation_data': True, 'alternatives': []},
            'Cons_Discr.':   {'index': None,     'etf': 'ESIC.L',  'currency': 'GBP', 'valuation_data': True, 'alternatives': []},
            'Cons_Staples':  {'index': None,     'etf': 'ESIS.L',  'currency': 'GBP', 'valuation_data': True, 'alternatives': []},
            'Energy':        {'index': None,     'etf': 'ENGE.L',  'currency': 'GBP', 'valuation_data': True, 'alternatives': []},
            'Defense':       {'index': None,     'etf': 'EUAD',    'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Financials':    {'index': None,     'etf': 'ESIF.L',  'currency': 'GBP', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':   {'index': None,     'etf': 'XDER.L',  'currency': 'GBp', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap':     {'index': '^STOXX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EXSA.DE','DX2X.DE']},
            'Small_Cap(EMU)':{'index': None, 'etf': 'SMEA.MI', 'currency':'EUR','valuation_data':True,'alternatives': ['IEUS']},
            'Value':         {'index': None, 'etf': 'IEVL.MI','currency':'EUR','valuation_data':True,'alternatives':['CEMS.DE']},
            'Momentum':      {'index': None, 'etf': 'CEMR.DE','currency':'EUR','valuation_data':True,'alternatives': ['IEFM.L']},
            'Quality':       {'index': None, 'etf': 'CEMQ.DE', 'currency':'EUR','valuation_data':True,'alternatives':['IEFQ.L']},
            'Min_Vol':       {'index': None, 'etf': 'EUMV',    'currency':'USD','valuation_data':True,'alternatives': ['IMV.L']},
            'EuroStoxx50':   {'index': '^STOXX50E','currency': 'EUR','valuation_data': False,'alternatives': ['FEZ','EXW1.DE']},
        }
    },

    'Germany': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWG', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Technology':   {'index': '^CXPHX','currency':'EUR','valuation_data':False, 'alternatives':['EXS2.DE']},
            'Healthcare':   {'index': '^CXPPX','currency':'EUR','valuation_data':False, 'alternatives': []},
            'Industrials':  {'index': '^CXPNX','currency':'EUR','valuation_data':False, 'alternatives': []},
            'Fin_Services': {'index': '^CXPVX','currency':'EUR','valuation_data':False, 'alternatives': []},
            'Insurance':    {'index': '^CXPIX','currency':'EUR','valuation_data':False, 'alternatives': []},
        },
        'factors': {
            'Large_Cap': {'index': '^GDAXI','currency':'EUR','valuation_data':False,'alternatives':['EXS1.DE']},
            'Mid_Cap':   {'index': '^MDAXI','currency':'EUR','valuation_data':False,'alternatives':['EXS3.DE']},
            'Small_Cap': {'index': '^SDAXI','currency':'EUR','valuation_data':False,'alternatives': []},
        }
    },

    'Japan': {
        'currency': 'JPY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWJ',   'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJ','HEWJ','BBJP','JPXN']},
            'Nikkei_225':   {'index': '^N225','currency': 'JPY', 'valuation_data': False,'alternatives': ['EWJ']},
            'Technology':    {'index': None, 'etf': '2854.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Semiconductors':{'index': None, 'etf': '2644.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Infrastructure_JP': {'index': None, 'etf': '2096.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Hotel_Retail_JP':   {'index': None, 'etf': '2098.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Logistics_JP':      {'index': None, 'etf': '2565.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Banks':         {'index': None, 'etf': '1615.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': ['1631.T']},
        # TOPIX-17 sector ETFs (JPY, Tokyo)
            'Foods':                     {'index': None, 'etf': '1617.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Energy_Resources':          {'index': None, 'etf': '1618.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Construction_Materials':    {'index': None, 'etf': '1619.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Raw_Materials_Chemicals':   {'index': None, 'etf': '1620.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Pharmaceuticals':           {'index': None, 'etf': '1621.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Autos_Transport_Equip':     {'index': None, 'etf': '1622.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Steel_Nonferrous':          {'index': None, 'etf': '1623.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Machinery':                 {'index': None, 'etf': '1624.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Electric_Precision':        {'index': None, 'etf': '1625.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'IT_Services_Others':        {'index': None, 'etf': '1626.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Electric_Power_Gas':        {'index': None, 'etf': '1627.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Transportation_Logistics':  {'index': None, 'etf': '1628.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Commercial_Wholesale':      {'index': None, 'etf': '1629.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Retail_Trade':              {'index': None, 'etf': '1630.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            # 'Banks' covered via 1615.T above; keep 1631.T as alternative there
            'Financials_ex_Banks':       {'index': None, 'etf': '1632.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':               {'index': None, 'etf': '1633.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'REITs':                      {'index': None, 'etf': '1488.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            'Large_Cap': {'index': None, 'etf': 'EWJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['DXJ']},
            'Small_Cap': {'index': None, 'etf': 'SCJ',  'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Value':     {'index': None, 'etf': 'EWJV', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Gov_Bonds': {'index': None, 'etf': '2561.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Biotech':   {'index': None, 'etf': '2639.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Quality':   {'index': None, 'etf': '1480.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Low_Vol':   {'index': None, 'etf': '1477.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Shareholder_Return': {'index': None, 'etf': '2529.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'High_Dividend_JP': {'index': None, 'etf': '2564.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': ['1577.T']},
            'Dividend_Growth_JP': {'index': None, 'etf': '1494.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'LowVol_HighDiv_JP': {'index': None, 'etf': '1399.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Agg_Bond_JP': {'index': None, 'etf': '2510.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': ['2511.T']},
        },
        'themes': {
            'Tech_JP':                 {'index': None, 'etf': '2854.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Digital_JP':              {'index': None, 'etf': '2626.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Robotics_JP':             {'index': None, 'etf': '2638.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Leisure_Entertainment_JP':{'index': None, 'etf': '2645.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Games_JP':                {'index': None, 'etf': '2640.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Retail_JP':               {'index': None, 'etf': '1617.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Topix':                   {'index': None, 'etf': '1306.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Topix30':                 {'index': None, 'etf': '1311.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Nikkei225_JP':            {'index': None, 'etf': '1321.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Prime150_JP':             {'index': None, 'etf': '2017.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'SmallMid_JP':             {'index': None, 'etf': '1493.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'MegaCap_JP':              {'index': None, 'etf': '2516.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'CleanTech_JP':            {'index': None, 'etf': '2637.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
            'Global_Leaders_JP':       {'index': None, 'etf': '2641.T', 'currency': 'JPY', 'valuation_data': True, 'alternatives': []},
        }
    },

    'Korea': {
        'currency': 'KRW',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWY', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Technology':    {'index': None, 'etf': '139260.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Semiconductors':{'index': None, 'etf': '091230.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Software':      {'index': None, 'etf': '157490.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Comm_Services': {'index': None, 'etf': '315270.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Cons_Discr.':   {'index': None, 'etf': '139290.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Cons_Staples':  {'index': None, 'etf': '227560.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Defense':       {'index': None, 'etf': '449450.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Energy':        {'index': None, 'etf': '139250.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Financials':    {'index': None, 'etf': '139270.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Insurance':     {'index': None, 'etf': '140700.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Healthcare':    {'index': None, 'etf': '227540.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Biotech':       {'index': None, 'etf': '244580.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':   {'index': None, 'etf': '476800.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Shipbuilding':  {'index': None, 'etf': '466920.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Automobiles':   {'index': None, 'etf': '091180.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {
            # Korea gov bonds via local ETF (approximation)
            'Gov_Bonds': {'index': None, 'etf': '385560.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': ['302190.KS']},
            'Dividend_Growth': {'index': None, 'etf': '211560.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'High_Dividend':   {'index': None, 'etf': '210780.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Momentum':        {'index': None, 'etf': '147970.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Quality':         {'index': None, 'etf': '275300.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Low_Vol':         {'index': None, 'etf': '215620.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Agg_Bond_KR':     {'index': None, 'etf': '273130.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            # 'Shareholder_Return' removed pending valid Yahoo symbol
        },
        'themes': {
            'Tech_KR':            {'index': None, 'etf': '139260.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Semiconductors_KR':  {'index': None, 'etf': '091230.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Robotics_KR':        {'index': None, 'etf': '445290.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Battery_KR':         {'index': None, 'etf': '305540.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Clean_Energy_KR':    {'index': None, 'etf': '381570.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Leisure_Travel_KR':  {'index': None, 'etf': '228800.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Consumer_KR':        {'index': None, 'etf': '139290.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Gaming_KR':          {'index': None, 'etf': '364990.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'E_Commerce_KR':      {'index': None, 'etf': '322400.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Internet_KR':        {'index': None, 'etf': '365000.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            # Representatives and industries
            'MSCI_Korea_Local':   {'index': None, 'etf': '310970.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Mega_Cap_KR':        {'index': None, 'etf': '122090.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Large_KR_200':       {'index': None, 'etf': '102110.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Mid_Cap_KR':         {'index': None, 'etf': '277650.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Media_KR':           {'index': None, 'etf': '266360.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Medical_Devices_KR': {'index': None, 'etf': '307510.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Securities_KR':      {'index': None, 'etf': '102970.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Steel_KR':           {'index': None, 'etf': '139240.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Cosmetics_KR':       {'index': None, 'etf': '228790.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
            'Shareholder_Return_KR': {'index': None, 'etf': '472720.KS', 'currency': 'KRW', 'valuation_data': True, 'alternatives': []},
        }
    },

    'China': {
        'currency': 'CNY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'FXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['MCHI','ASHR','GXC']},
            'Broad_Market_US_ETF': {'index': None, 'etf': 'MCHI', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['FXI','GXC','ASHR']},
            'Technology':   {'index': None, 'etf': 'CQQQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['KWEB','159939.SZ']},
            # China Semiconductors (03191.HK) removed pending valid Yahoo symbol
            'Consumer':     {'index': None, 'etf': 'CHIQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Cons_Staples': {'index': None, 'etf': 'CHIS', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['159928.SZ']},
            'Financials':   {'index': None, 'etf': 'CHIX', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Financials_Local': {'index': None, 'etf': '510230.SS', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'Industrials':  {'index': None, 'etf': 'CHII', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Materials':    {'index': None, 'etf': 'CHIM', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Energy':       {'index': None, 'etf': 'CHIE', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Utilities':    {'index': None, 'etf': 'CHIU', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['159611.SZ']},
            'Comm_Services':{'index': None, 'etf': 'CHIC', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Real_Estate':  {'index': None, 'etf': 'CHIR', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['512200.SS']},
            'Healthcare':   {'index': None, 'etf': 'CHIH', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['KURE','512010.SS']},
            'Banks':        {'index': None, 'etf': 'CHIX', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Banks_Local':  {'index': None, 'etf': '512800.SS', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'Biotech':      {'index': None, 'etf': '2820.HK', 'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Software':     {'index': None, 'etf': '159852.SZ', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'Telecom_Equip_CN': {'index': None, 'etf': '515880.SS', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'Food_Beverage_CN': {'index': None, 'etf': '159736.SZ', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'Pharma_CN':        {'index': None, 'etf': '159992.SZ', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            # China Defense (512660.CH) removed pending valid Yahoo symbol
            'Insurance':    {'index': None, 'etf': None, 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            # China Financials local added as 510230.SS above
        },
        'factors': {
            'Large_Cap': {'index': None, 'etf': 'FXI',  'currency': 'USD', 'valuation_data': True, 'alternatives': ['GXC']},
            'Small_Cap': {'index': None, 'etf': 'ECNS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'A_Shares':  {'index': None, 'etf': 'ASHR', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['CNYA','KBA']},
            'CSI_300':   {'index': '000300.SS','currency': 'CNY','valuation_data': False,'alternatives': ['ASHR']},
            'Growth':    {'index': None, 'etf': 'CNXT', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            # Disabled: no reliable Yahoo price history for 02803.HK (YHD placeholder). No clean Quality CN ETF found.
            'Quality_CN': {'index': None, 'etf': None, 'currency': 'HKD', 'valuation_data': False, 'alternatives': []},
            'Low_Vol_CN': {'index': None, 'etf': '515300.SS', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'High_Dividend_HK': {'index': None, 'etf': '3070.HK', 'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'LowVol_HighDiv_CN': {'index': None, 'etf': '515100.SS', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
        },
        'themes': {
            'AI':                   {'index': None, 'etf': '159819.SZ', 'currency': 'CNY', 'valuation_data': True, 'alternatives': []},
            'Semiconductors_CN':    {'index': None, 'etf': '3191.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Tech_HangSeng':        {'index': None, 'etf': '3033.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Robotics_CN':          {'index': None, 'etf': '2807.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Battery_CN':           {'index': None, 'etf': '2845.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Clean_Energy_CN':      {'index': None, 'etf': '2809.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Internet_HK':          {'index': None, 'etf': '2812.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'IPO_HK':               {'index': None, 'etf': '2835.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'New_Economy_CN':       {'index': None, 'etf': '3173.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Cloud_CN':             {'index': None, 'etf': '2826.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Solar_CN_HK':          {'index': None, 'etf': '3134.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Global_Leaders_HK':    {'index': None, 'etf': '3050.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Consumer_Services_HK': {'index': None, 'etf': '3167.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'MSCI_CN_HK':           {'index': None, 'etf': '2801.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'A50_HK':               {'index': None, 'etf': '2823.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'CSI300_HK':            {'index': None, 'etf': '3188.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'HSCEI_HK':             {'index': None, 'etf': '2828.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'HSI_HK':               {'index': None, 'etf': '2800.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'STAR50_HK':            {'index': None, 'etf': '3151.HK',   'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
        }
    },

    'Canada': {
        'currency': 'CAD',
        'sectors': {
            'Broad_Market': {'index': '^GSPTSE', 'currency': 'CAD', 'valuation_data': False, 'alternatives': ['XIC.TO','EWC']},
            'Broad_Market_ETF': {'index': None, 'etf': 'EWC', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Brazil': {
        'currency': 'BRL',
        'sectors': {
            'Broad_Market': {'index': '^BVSP', 'currency': 'BRL', 'valuation_data': False, 'alternatives': ['EWZ']},
        },
        'factors': {}
    },

    'Mexico': {
        'currency': 'MXN',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWW', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Argentina': {
        'currency': 'ARS',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'ARGT', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Colombia': {
        'currency': 'COP',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'ICOL', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['GXG','COLO']},
        },
        'factors': {}
    },

    'Global_ExUS': {
        'currency': 'USD',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'ACWX', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['VEU','VEA','IEFA']},
            'Broad_ExUS':   {'index': None, 'etf': 'IXUS', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['VEU']},
            'Developed_exUS': {'index': None, 'etf': 'EFA', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['VEA','IEFA']},
            'Emerging_Markets': {'index': None, 'etf': 'EEM', 'currency': 'USD', 'valuation_data': True, 'alternatives': ['VWO']},
            'EM_ex_China': {'index': None, 'etf': 'EMXC', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Pacific_Developed': {'index': None, 'etf': 'VPL', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Latin_America': {'index': None, 'etf': 'ILF', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
            'Eurozone': {'index': None, 'etf': 'EZU', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Singapore': {
        'currency': 'SGD',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Indonesia': {
        'currency': 'IDR',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EIDO', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Thailand': {
        'currency': 'THB',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'THD', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Malaysia': {
        'currency': 'MYR',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWM', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Vietnam': {
        'currency': 'VND',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'VNM', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'India': {
        'currency': 'INR',
        'sectors': {
            'Broad_Market': {'index': '^NSEI', 'currency': 'INR', 'valuation_data': False, 'alternatives': ['INDA','EPI','INDY']},
            'Financials':   {'index': None,    'etf': 'INDF', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Consumer':     {'index': None,    'etf': 'INCO', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Technology':   {'index': None,    'etf': 'INQQ', 'currency': 'USD', 'valuation_data': True,  'alternatives': []},
            'Small_Cap':    {'index': None,    'etf': 'SMIN', 'currency': 'USD', 'valuation_data': True,  'alternatives': ['SCIF']},
            'Value(Proxy)': {'index': None,    'etf': 'EPI',  'currency': 'USD', 'valuation_data': True,  'alternatives': []},
        },
        'factors': {}
    },

    'UK': {
        'currency': 'GBP',
        'sectors': {
            'Broad_Market': {'index': '^FTSE', 'currency': 'GBP', 'valuation_data': False, 'alternatives': ['ISF.L','VUKE.L']},
            'Broad_Market_ETF': {'index': None, 'etf': 'EWU', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'France': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^FCHI', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWQ']},
            'Broad_Market_ETF': {'index': None, 'etf': 'EWQ', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Italy': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': 'FTSEMIB.MI', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWI']},
            'Broad_Market_ETF': {'index': None, 'etf': 'EWI', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Spain': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': '^IBEX', 'currency': 'EUR', 'valuation_data': False, 'alternatives': ['EWP']},
            'Broad_Market_ETF': {'index': None, 'etf': 'EWP', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Taiwan': {
        'currency': 'TWD',
        'sectors': {
            'Broad_Market':     {'index': '^TWII', 'currency': 'TWD', 'valuation_data': False, 'alternatives': ['EWT']},
            'Broad_Market_ETF': {'index': None, 'etf': 'EWT', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Hong_Kong': {
        'currency': 'HKD',
        'sectors': {
            'Broad_Market': {'index': '^HSI', 'currency': 'HKD', 'valuation_data': False, 'alternatives': ['EWH']},
        },
        'factors': {
            'Hang_Seng_Tech': {'index': None, 'etf': '3033.HK', 'currency': 'HKD', 'valuation_data': True, 'alternatives': []},
            'Gov_Bonds_HK':   {'index': None, 'etf': '3199.HK', 'currency': 'HKD', 'valuation_data': True, 'alternatives': []}
        }
    },

    'Australia': {
        'currency': 'AUD',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWA', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Sweden': {
        'currency': 'SEK',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWD', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Greece': {
        'currency': 'EUR',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'GREK', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Switzerland': {
        'currency': 'CHF',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EWL', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Israel': {
        'currency': 'ILS',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'EIS', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },

    'Turkey': {
        'currency': 'TRY',
        'sectors': {
            'Broad_Market': {'index': None, 'etf': 'TUR', 'currency': 'USD', 'valuation_data': True, 'alternatives': []},
        },
        'factors': {}
    },
}

# (Validation and analysis helpers removed: this module focuses on data collection only.)

# ============================
# Korean Labels (Countries/Sectors/Factors)
# ============================

KOREAN_LABELS = {
    'countries': {
        'Global': '글로벌',
        'US': '미국',
        'Global_ExUS': '글로벌(미국 제외)',
        'Europe': '유럽',
        'Germany': '독일',
        'UK': '영국',
        'France': '프랑스',
        'Italy': '이탈리아',
        'Spain': '스페인',
        'Japan': '일본',
        'China': '중국',
        'Hong_Kong': '홍콩',
        'Taiwan': '대만',
        'Korea': '한국',
        'India': '인도',
        'Canada': '캐나다',
        'Brazil': '브라질',
        'Singapore': '싱가포르',
        'Indonesia': '인도네시아',
        'Thailand': '태국',
        'Malaysia': '말레이시아',
        'Australia': '호주',
        'Sweden': '스웨덴',
        'Greece': '그리스',
        'Switzerland': '스위스',
        'Israel': '이스라엘',
        'Turkey': '튀르키예',
        'Mexico': '멕시코',
        'Argentina': '아르헨티나',
        'Colombia': '콜롬비아',
        'Vietnam': '베트남',
        'Commodities': '원자재',
    },
    'sectors': {
        'Broad_Market': '전반 시장',
        'Technology': '정보기술',
        'Healthcare': '헬스케어',
        'Financials': '금융',
        'Fin_Services': '금융서비스',
        'Insurance': '보험',
        'Cons_Discr.': '경기소비재',
        'Cons_Staples': '필수소비재',
        'Industrials': '산업재',
        'Energy': '에너지',
        'Materials': '소재',
        'Utilities': '유틸리티',
        'Comm_Services': '커뮤니케이션서비스',
        'Real_Estate': '부동산',
        'Telecom': '통신',
        'Automobiles': '자동차',
        'Basic_Resrcs': '기초자원',
        'Construction': '건설',
        'Nikkei_225': '니케이 225',
        # Commodities detail
        'Broad': '원자재 전반',
        'Energy_Broad': '에너지 전반',
        'Crude_Oil': '원유',
        'Natural_Gas': '천연가스',
        'Gold': '금',
        'Silver': '은',
        'Palladium': '팔라듐',
        'Platinum': '플래티넘',
        'Copper': '구리',
        'Industrial_Metals': '산업금속',
        'Agriculture_Broad': '농산물 전반',
        'Dry_Bulk_Freight': 'BDI 운임지수',
        'Carbon_Global': '탄소배출권(글로벌)',
        'Carbon_California': '탄소배출권(캘리포니아)',
        'Volatility': '변동성',
    },
    'factors': {
        'Global_BM': '글로벌 BM',
        'Global_Treasury': '글로벌 국채',
        'Dividend_Growth_Global': '글로벌 배당성장',
        'High_Dividend_Global': '글로벌 고배당',
        'Momentum_Developed': '선진국 모멘텀',
        'Quality_Global': '글로벌 퀄리티',
        'IPO_Global': '글로벌 신규상장',
        'Moat_Global': '글로벌 해자',
        'CashFlow_Global': '글로벌 현금흐름',
        'Min_Vol_Global': '글로벌 최소변동성',
        # Global bonds
        'Green_Bonds_Global': '글로벌 그린본드',
        'EM_Local_Bonds': '이머징 로컬채',
        'EM_USD_Bonds': '이머징 USD채',
        'Large_Cap': '대형주',
        'Mid_Cap': '중형주',
        'Small_Cap': '소형주',
        'Small_Cap_600': '소형주 600',
        'Nasdaq_100': '나스닥 100',
        'Micro_Cap': '마이크로캡',
        'Small_Value': '소형 가치',
        'Small_Growth': '소형 성장',
        'Mid_Value': '중형 가치',
        'Mid_Growth': '중형 성장',
        'Equal_Weight': '동일가중',
        'Low_Vol': '저변동성',
        'High_Beta': '하이베타',
        'Low_Beta': '로우베타',
        'High_Dividend': '고배당',
        'Dividend_Growth': '배당성장',
        'Value': '가치',
        'Growth': '성장',
        'Momentum': '모멘텀',
        'Quality': '퀄리티',
        'High_Quality': '고퀄리티',
        'Profitability': '수익성',
        'Multi_Factor': '멀티팩터',
        'Wide_Moat': '와이드 모트',
        'Semiconductors': '반도체',
        'US_Power': '미국 전력',
        'China_Tech_US': '중국 테크',
        'Defense_Tech': '방위 기술',
        'Manufacturing_Trad': '전통 제조업',
        'Pipelines': '파이프라인',
        'Dividend_Aristocrats': '배당귀족',
        'Preferreds': '우선주',
        'US_IG_Corp': '미 IG 회사채',
        'US_HY_Corp': '미 HY 회사채',
        'US_Treasuries_20Y': '미국 20년물 국채',
        'A_Shares': '중국 A주',
        'CSI_300': 'CSI 300',
        'Gov_Bonds': '국채',
        'Broad_ExUS': '광범위(미국 제외)',
        'Developed_exUS': '선진국(미국 제외)',
        'Emerging_Markets': '신흥국',
        'EM_ex_China': '신흥국(중국 제외)',
        'Hang_Seng_Tech': '항셍 테크',
        # US factors extended
        'Dividend_Growth_US': '미국 배당성장',
        'High_Dividend_US': '미국 고배당',
        'IPO_US': '미국 신규상장',
        'Buyback_US': '미국 주주환원',
        'Agg_Bond_US': '미국 채권 전반',
        'TIPS_US': '미국 TIPS',
        'Convertibles_US': '미국 전환사채',
        'MBS_US': '미국 MBS',
        'Senior_Loans_US': '미국 시니어론',
        'Fallen_Angels_US': '미국 강등채',
        # Japan
        'High_Dividend_JP': '일본 고배당',
        'Dividend_Growth_JP': '일본 배당성장',
        'Agg_Bond_JP': '일본 채권 전반',
        # Korea
        'Agg_Bond_KR': '한국 종합채',
        # China
        'Quality_CN': '중국 퀄리티',
        'Low_Vol_CN': '중국 저변동성',
        'HY_Bond_HK': '중국 달러 HY채',
        'Quasi_Govt_HK': '홍콩 준정부채',
        'Gov_Bonds_HK': '홍콩 국채',
        'Financials_Local': '중국 금융(본토)',
        'Banks_Local': '중국 은행(본토)',
        'Mega_Cap_US': '미국 초대형주',
        'Nasdaq100_EW': '나스닥100 동일가중',
        'Dow_30': '다우 30',
        'Magnificent7_US': '매그니피센트 7',
    },
    'themes': {
        # Disruptive Tech
        'Battery': '2차 전지',
        '5G': '5G',
        'AI': '인공지능',
        'IoT': '사물인터넷',
        'Gaming': '게임',
        'Robotics': '로봇',
        'Data_Center_REITs': '데이터 센터 리츠',
        'Blockchain': '블록체인',
        'Bitcoin_Spot': '비트코인 현물',
        'Cybersecurity': '사이버 보안',
        'Smart_Factory': '스마트 공장',
        'Smart_Grid': '스마트 그리드',
        'Quantum_Computing': '양자컴퓨팅',
        'Space_Exploration': '우주 탐사',
        'Autonomous_Vehicles': '자율주행',
        'Digital_Payments': '전자 결제',
        'Cloud': '클라우드',
        'Exponential_Tech': '혁신 기술',
        # Demographics & Culture
        'Online_Retail': '온라인 리테일',
        'Aging_Longevity': '고령화·장수',
        'Digital_Health': '디지털 헬스',
        'Luxury': '럭셔리',
        'Millennials': '밀레니얼',
        'Obesity_Treatment': '비만 치료',
        'India_Consumer': '인도 소비',
        'Genomics': '유전체',
        'Pet_Care': '펫케어',
        'Internet_KR': '한국 인터넷',
        # Physical Environment
        'Hydrogen': '수소 산업',
        'Water_Infrastructure': '수자원 인프라',
        'AgTech': '애그테크',
        'Uranium_Nuclear': '우라늄·원자력',
        'Clean_Energy': '클린에너지',
        'CleanTech': '클린테크',
        'Solar': '태양광',
        'Disruptive_Materials': '디스럽티브 소재',
        'Rare_Earths': '희귀 광물',
        # Korea themes extended
        'MSCI_Korea_Local': 'MSCI 한국(현지)',
        'Mega_Cap_KR': '한국 초대형주',
        'Large_KR_200': '한국 대형(200)',
        'Mid_Cap_KR': '한국 중형주',
        'Media_KR': '한국 미디어',
        'Medical_Devices_KR': '한국 의료기기',
        'Securities_KR': '한국 증권',
        'Steel_KR': '한국 철강',
        'Cosmetics_KR': '한국 화장품',
        'Shareholder_Return_KR': '한국 주주환원',
        # China/HK themes
        'Internet_HK': '홍콩 인터넷',
        'IPO_HK': '홍콩 IPO',
        'New_Economy_CN': '중국 신경제',
        'Cloud_CN': '중국 클라우드',
        'Solar_CN_HK': '태양광(중국/HK)',
        'Global_Leaders_HK': '홍콩 글로벌리더',
        'Consumer_Services_HK': '홍콩 소비·서비스',
        'MSCI_CN_HK': '중국 MSCI(홍콩)',
        'A50_HK': '중국 A50(홍콩)',
        'CSI300_HK': 'CSI 300(홍콩)',
        'HSCEI_HK': 'HSCEI(홍콩)',
        'HSI_HK': 'HSI(홍콩)',
        'STAR50_HK': 'STAR 50(홍콩)',
        # Japan themes extended
        'Topix': '일본 TOPIX',
        'Topix30': '일본 TOPIX 30',
        'Nikkei225_JP': '일본 니케이 225',
        'Prime150_JP': '일본 프라임 150',
        'SmallMid_JP': '일본 중소형',
        'MegaCap_JP': '일본 초대형',
        'CleanTech_JP': '일본 클린테크',
        'Global_Leaders_JP': '일본 글로벌리더',
    },
}

def get_korean_label(kind: str, key: str) -> str:
    """Return Korean label for a given kind ('countries'|'sectors'|'factors') and key.
    Falls back to key if no mapping exists.
    """
    try:
        return KOREAN_LABELS.get(kind, {}).get(key, key)
    except Exception:
        return key

# ============================
# Safe add helpers (dedupe + Korea US-theme skip)
# ============================

def symbol_exists_in_universe(universe: dict, symbol: str) -> bool:
    if not symbol:
        return False
    for _, cdata in universe.items():
        for section in ("sectors", "factors"):
            for _, asset in cdata.get(section, {}).items():
                sym = asset.get("index") or asset.get("etf")
                if isinstance(sym, str) and sym == symbol:
                    return True
    return False

def should_skip_asset(country: str, name: str, asset: dict, universe: dict) -> tuple[bool, str | None]:
    """Return (skip, reason). Rules:
    - Skip if symbol already exists anywhere in universe
    - Skip Korea entries that are US-themed local listings (name endswith '_K' or startswith 'US_')
    """
    sym = (asset or {}).get("etf") or (asset or {}).get("index")
    if sym and symbol_exists_in_universe(universe, sym):
        return True, "duplicate_symbol"
    if country == 'Korea' and (name.endswith('_K') or name.startswith('US_')):
        return True, "korea_us_theme_skipped"
    return False, None

# ============================
# Daily CSV Cache (New)
# ============================
import os
from pathlib import Path as _Path
import time
import re
import json as _json
import io as _io
import subprocess as _sp

_BASE_DIR = _Path(__file__).resolve().parent
_DATA_DIR = _BASE_DIR / "data" / "daily"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Currency overrides (auto-resolved from Yahoo audit)
_CUR_OVERRIDES_PATH = _BASE_DIR / "data" / "currency_overrides.json"

def _load_currency_overrides() -> dict:
    try:
        import json as _json
        if _CUR_OVERRIDES_PATH.exists():
            return _json.loads(_CUR_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def resolve_currency(symbol: str, expected: str | None) -> str | None:
    """Return corrected currency for symbol if an override exists; else expected.

    Use this when exporting catalogs or performing FX conversion to ensure
    we use the actual trading currency detected from Yahoo metadata.
    """
    overrides = _load_currency_overrides()
    return overrides.get(symbol, expected)

def _sanitize_symbol(symbol: str) -> str:
    """Filesystem-safe filename for a Yahoo symbol."""
    if symbol.startswith('^'):
        symbol = 'IDX_' + symbol[1:]
    return re.sub(r"[^A-Za-z0-9._-]", "_", symbol)

def _csv_path_for(symbol: str) -> _Path:
    return _DATA_DIR / f"{_sanitize_symbol(symbol)}.csv"

def _load_existing_csv(path: _Path):
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
        sanitized = False
        if "<<<<<<<" in text or "=======" in text or ">>>>>>>" in text:
            # Drop git conflict marker lines
            lines = []
            for ln in text.splitlines():
                if ln.startswith("<<<<<<<") or ln.startswith("=======") or ln.startswith(">>>>>>>"):
                    sanitized = True
                    continue
                lines.append(ln)
            text = "\n".join(lines)
        # Use StringIO to parse
        df = pd.read_csv(_io.StringIO(text))
        # Ensure Date column is present
        if "Date" not in df.columns:
            return None
        # Coerce to datetime and set as index
        dt = pd.to_datetime(df["Date"], errors="coerce", utc=True)
        try:
            dt = dt.dt.tz_convert(None)
        except Exception:
            pass
        df["Date"] = dt
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df = df[~df.index.duplicated(keep='last')]
        # Attach attr so caller can decide to rewrite
        try:
            df.attrs["sanitized"] = sanitized
        except Exception:
            pass
        return df
    except Exception:
        return None


# ============================
# CSV Sanitization Utilities
# ============================

def _strip_conflict_markers(text: str) -> tuple[str, bool]:
    """Remove git merge conflict markers and duplicate header lines.

    Returns (sanitized_text, changed?).
    - Drops lines starting with '<<<<<<<', '=======', '>>>>>>>'
    - Keeps only the first header line beginning with 'Date,'
    """
    lines = text.splitlines()
    out = []
    changed = False
    header_seen = False
    for ln in lines:
        if ln.startswith("<<<<<<<") or ln.startswith("=======") or ln.startswith(">>>>>>>"):
            changed = True
            continue
        if ln.startswith("Date,"):
            if header_seen:
                changed = True
                continue
            header_seen = True
        out.append(ln)
    sanitized = "\n".join(out)
    return sanitized, changed or (sanitized != text)


def sanitize_daily_csv_file(path: _Path) -> dict:
    """Sanitize a single daily CSV file in-place if it contains merge markers or header duplication.

    Returns summary dict: {file, changed, rows_before, rows_after, note}
    """
    info = {"file": str(path), "changed": False, "rows_before": None, "rows_after": None, "note": None}
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as e:
        info.update({"note": f"read_error: {str(e)[:120]}"})
        return info

    sanitized_text, changed = _strip_conflict_markers(raw)
    # If no change and file looks fine, try quick parse for dates; if parse is fine, skip
    if not changed:
        try:
            df0 = pd.read_csv(_io.StringIO(sanitized_text))
            if "Date" in df0.columns:
                df0["Date"] = pd.to_datetime(df0["Date"], errors="coerce")
                if df0["Date"].notna().any():
                    info.update({"rows_before": int(df0["Date"].notna().sum()), "rows_after": int(df0["Date"].notna().sum())})
                    return info
        except Exception:
            # fall-through to rewrite from parsed DataFrame
            pass

    # Parse with pandas and rewrite canonical CSV
    try:
        df = pd.read_csv(_io.StringIO(sanitized_text))
        info["rows_before"] = int(len(df))
        if "Date" not in df.columns:
            # If missing Date, we cannot normalize; write sanitized text only
            path.write_text(sanitized_text, encoding="utf-8")
            info.update({"changed": changed, "rows_after": info["rows_before"], "note": "sanitized_text_only"})
            return info
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df = df[~df.index.duplicated(keep='last')]
        tmp = path.with_suffix('.csv.tmp')
        df.to_csv(tmp, index_label="Date")
        os.replace(tmp, path)
        info.update({"changed": True, "rows_after": int(len(df))})
        return info
    except Exception as e:
        # Fallback: write sanitized text if parsing failed
        try:
            path.write_text(sanitized_text, encoding="utf-8")
            info.update({"changed": changed, "rows_after": info["rows_before"], "note": f"parse_failed:{str(e)[:80]}"})
        except Exception as e2:
            info.update({"note": f"write_error:{str(e2)[:80]}"})
        return info


def sanitize_all_daily_csvs() -> pd.DataFrame:
    """Scan global_universe/data/daily for CSVs and sanitize all in-place.

    Writes summary to data/daily_sanitization_summary.csv
    """
    rows = []
    for p in sorted(_DATA_DIR.glob("*.csv")):
        res = sanitize_daily_csv_file(p)
        rows.append(res)
    df = pd.DataFrame(rows)
    try:
        ts = datetime.now(_ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    except Exception:
        ts = datetime.now().isoformat()
    df["run_at"] = ts
    out = _BASE_DIR / "data" / "daily_sanitization_summary.csv"
    df.to_csv(out, index=False)
    return df

def _fetch_history(symbol: str, start: datetime | None = None, end: datetime | None = None, max_retries: int = 3, pause: float = 0.5) -> pd.DataFrame:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            t = yf.Ticker(symbol)
            if start is None and end is None:
                # Try a sequence of periods from longest to shortest; some symbols only allow 1d/5d
                periods = ["max", "10y", "5y", "2y", "1y", "6mo", "3mo", "1mo", "5d", "1d"]
                hist = pd.DataFrame()
                last_exc = None
                for p in periods:
                    try:
                        h = t.history(period=p, interval="1d")
                        if h is not None and not h.empty:
                            hist = h
                            break
                    except Exception as e:
                        last_exc = e
                        continue
                if hist is None or hist.empty:
                    # yfinance failed (common on CI). Fallback to Yahoo chart API via curl
                    for p in periods:
                        h = _fetch_history_via_chart(symbol, period=p)
                        if h is not None and not h.empty:
                            hist = h
                            break
                    if hist is None or hist.empty:
                        if last_exc:
                            raise last_exc
                        return pd.DataFrame()
            else:
                # Incremental fetch with explicit end to avoid start > end errors
                today = datetime.utcnow()
                if end is None:
                    end = today
                if start is not None and start.date() > end.date():
                    # Nothing to fetch yet
                    return pd.DataFrame()
                kwargs = {"interval": "1d", "start": start.strftime("%Y-%m-%d") if start else None, "end": end.strftime("%Y-%m-%d") if end else None}
                # Remove Nones
                kwargs = {k: v for k, v in kwargs.items() if v is not None}
                try:
                    hist = t.history(**kwargs)
                except Exception:
                    # As a fallback, try a short period if range query fails
                    hist = t.history(period="5d", interval="1d")
                    if hist is None or hist.empty:
                        # fallback to chart api for a short period
                        hist = _fetch_history_via_chart(symbol, period="5d")
            if not hist.empty:
                cols = [c.strip().title() for c in hist.columns]
                hist.columns = cols
                keep = [c for c in ["Open","High","Low","Close","Adj Close","Volume"] if c in hist.columns]
                hist = hist[keep]
            return hist
        except Exception as e:
            last_err = e
            time.sleep(min(10, pause * (2 ** (attempt - 1))))
    raise RuntimeError(f"Failed to fetch history for {symbol}: {last_err}")

def update_symbol_csv(symbol: str, pause: float = 0.6, lookback_days: int = 0) -> tuple[_Path, int, bool]:
    path = _csv_path_for(symbol)
    existing = _load_existing_csv(path)
    if existing is None or existing.empty:
        df = _fetch_history(symbol, pause=pause)
        if df is None or df.empty:
            # write empty placeholder
            pd.DataFrame(columns=["Open","High","Low","Close","Adj Close","Volume"]).to_csv(path)
            return path, 0, False
        tmp = path.with_suffix('.csv.tmp')
        df.to_csv(tmp, index_label="Date")
        os.replace(tmp, path)
        return path, len(df), True
    last = existing.index.max()
    # Re-fetch recent window to capture corrections/splits/dividends
    lb = int(max(0, lookback_days))
    start = (last - timedelta(days=lb)) if lb > 0 else (last + timedelta(days=1))
    new = _fetch_history(symbol, start=start, pause=pause)
    if new is None or new.empty:
        # If existing was sanitized (conflict markers removed), rewrite file even without new rows
        sanitized = bool(getattr(existing, 'attrs', {}).get('sanitized', False))
        if sanitized:
            tmp = path.with_suffix('.csv.tmp')
            existing.to_csv(tmp, index_label="Date")
            os.replace(tmp, path)
            return path, 0, True
        return path, 0, False

    # Merge and detect overlap changes
    combined = pd.concat([existing, new])
    combined = combined[~combined.index.duplicated(keep='last')].sort_index()
    added_rows = len(combined) - len(existing)
    changed_recent = False
    try:
        overlap_idx = existing.index.intersection(new.index)
        if len(overlap_idx) > 0:
            cols = [c for c in ["Open","High","Low","Close","Adj Close","Volume"] if c in combined.columns and c in existing.columns]
            if cols:
                before = existing.loc[overlap_idx, cols]
                after = combined.loc[overlap_idx, cols]
                # Normalize dtypes to avoid false diffs
                before = before.astype(float, errors='ignore') if hasattr(before, 'astype') else before
                after = after.astype(float, errors='ignore') if hasattr(after, 'astype') else after
                changed_recent = not before.equals(after)
    except Exception:
        changed_recent = True  # conservative
    tmp = path.with_suffix('.csv.tmp')
    combined.to_csv(tmp, index_label="Date")
    os.replace(tmp, path)
    return path, added_rows if added_rows > 0 else 0, changed_recent

def backfill_symbol_csv(symbol: str, pause: float = 0.6) -> tuple[_Path, int, int]:
    """Fetch the longest-available history for `symbol` and merge with any existing CSV.

    Returns (path, added_rows, total_rows_after).
    Writes an empty placeholder file if no history is available.
    """
    path = _csv_path_for(symbol)
    existing = _load_existing_csv(path)
    try:
        df = _fetch_history(symbol, pause=pause)
    except Exception:
        df = pd.DataFrame()
    if df is None or df.empty:
        # Ensure file exists even if empty, for monitoring
        if existing is None:
            pd.DataFrame(columns=["Open","High","Low","Close","Adj Close","Volume"]).to_csv(path)
            return path, 0, 0
        else:
            return path, 0, len(existing)
    if existing is None or existing.empty:
        tmp = path.with_suffix('.csv.tmp')
        df.to_csv(tmp, index_label="Date")
        os.replace(tmp, path)
        return path, len(df), len(df)
    combined = pd.concat([existing, df])
    combined = combined[~combined.index.duplicated(keep='last')].sort_index()
    added = len(combined) - len(existing)
    tmp = path.with_suffix('.csv.tmp')
    combined.to_csv(tmp, index_label="Date")
    os.replace(tmp, path)
    return path, added, len(combined)

def list_all_symbols(universe: dict) -> list[str]:
    """Return all symbols including index, etf, and alternatives (deduped)."""
    symbols: list[str] = []
    for _, country_data in universe.items():
        for section in ("sectors", "factors", "themes"):
            for _, asset in country_data.get(section, {}).items():
                for key in ("index", "etf"):
                    v = asset.get(key)
                    if v:
                        symbols.append(str(v))
                for alt in asset.get("alternatives", []) or []:
                    if alt:
                        symbols.append(str(alt))
    # Deduplicate preserving order
    seen = set(); out = []
    for s in symbols:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def analyze_price_file(symbol: str) -> dict:
    """Compute basic quality metrics for a symbol's price CSV."""
    path = _csv_path_for(symbol)
    out = {
        "symbol": symbol,
        "file": str(path),
        "exists": path.exists(),
        "rows": 0,
        "first_date": None,
        "last_date": None,
        "days_span": None,
        "has_adj_close": False,
        "missing_close_ratio": None,
        "status": None,
        "note": None,
    }
    if not path.exists():
        out.update({"status": "missing_file", "note": "no_csv"})
        return out
    try:
        df = pd.read_csv(path)
    except Exception as e:
        out.update({"status": "error", "note": f"read_error: {str(e)[:120]}"})
        return out
    if df.empty or "Date" not in df.columns:
        out.update({"status": "empty", "rows": 0, "note": "no_rows"})
        return out
    try:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        out["rows"] = len(df)
        out["first_date"] = df["Date"].iloc[0].date().isoformat()
        out["last_date"] = df["Date"].iloc[-1].date().isoformat()
        out["days_span"] = (df["Date"].iloc[-1] - df["Date"].iloc[0]).days + 1
        out["has_adj_close"] = ("Adj Close" in df.columns)
        if "Close" in df.columns:
            miss = pd.isna(df["Close"]).sum()
            out["missing_close_ratio"] = float(miss) / float(len(df)) if len(df) else None
        # Derive basic status
        if out["rows"] <= 0:
            out["status"] = "empty"
            out["note"] = "no_rows"
        elif out["rows"] == 1 or (out["days_span"] is not None and out["days_span"] <= 1):
            out["status"] = "single_day"
            out["note"] = "only_1_day"
        elif out["rows"] < 5:
            out["status"] = "few_rows"
            out["note"] = "lt_5_rows"
        else:
            out["status"] = "ok"
    except Exception as e:
        out.update({"status": "error", "note": f"analyze_error: {str(e)[:120]}"})
    return out

def backfill_all_prices(universe: dict, pause: float = 0.6, symbols: list[str] | None = None) -> pd.DataFrame:
    """Backfill price history to the maximum available for the given symbols and write a monitoring summary."""
    if symbols is None:
        symbols = list_all_symbols(universe)
    results = []
    for sym in symbols:
        try:
            path, added, total = backfill_symbol_csv(sym, pause=pause)
            q = analyze_price_file(sym)
            q.update({
                "added": added,
                "total_rows": total,
            })
            results.append(q)
        except Exception as e:
            results.append({
                "symbol": sym,
                "file": str(_csv_path_for(sym)),
                "exists": _csv_path_for(sym).exists(),
                "rows": 0,
                "first_date": None,
                "last_date": None,
                "days_span": None,
                "has_adj_close": False,
                "missing_close_ratio": None,
                "status": "error",
                "note": str(e)[:200],
                "added": 0,
                "total_rows": None,
            })
        time.sleep(pause)
    df = pd.DataFrame(results)
    try:
        ts = datetime.now(_ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    except Exception:
        ts = datetime.now().isoformat()
    df["run_at"] = ts
    out_path = _BASE_DIR / "data" / "prices_backfill_summary.csv"
    df.to_csv(out_path, index=False)
    return df

def list_primary_symbols(universe: dict) -> list[str]:
    """
    Return one primary symbol per asset: prefer 'index' if present else 'etf'.
    Skips alternatives.
    """
    symbols: list[str] = []
    for _, country_data in universe.items():
        for section in ("sectors", "factors"):
            for _, asset in country_data.get(section, {}).items():
                sym = asset.get("index") or asset.get("etf")
                if sym:
                    symbols.append(str(sym))
    # Deduplicate preserving order
    seen = set(); out = []
    for s in symbols:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def build_symbols_catalog(universe: dict, primary_only: bool = True) -> pd.DataFrame:
    rows = []
    for country, cdata in universe.items():
        for section in ("sectors", "factors", "themes"):
            for name, asset in cdata.get(section, {}).items():
                if primary_only:
                    key = "index" if asset.get("index") else ("etf" if asset.get("etf") else None)
                    if key:
                        v = asset[key]
                        rows.append({
                            "country": country,
                            "category": section,
                            "name": name,
                            "field": key,
                            "symbol": v,
                            "currency": asset.get("currency"),
                            "resolved_currency": resolve_currency(v, asset.get("currency")),
                            "file": str(_csv_path_for(v)),
                        })
                else:
                    for key in ("index", "etf"):
                        v = asset.get(key)
                        if v:
                            rows.append({
                                "country": country,
                                "category": section,
                                "name": name,
                                "field": key,
                                "symbol": v,
                                "currency": asset.get("currency"),
                                "resolved_currency": resolve_currency(v, asset.get("currency")),
                                "file": str(_csv_path_for(v)),
                            })
                    for alt in asset.get("alternatives", []) or []:
                        rows.append({
                            "country": country,
                            "category": section,
                            "name": name,
                            "field": "alternative",
                            "symbol": alt,
                            "currency": asset.get("currency"),
                            "resolved_currency": resolve_currency(alt, asset.get("currency")),
                            "file": str(_csv_path_for(alt)),
                        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["field","symbol"]).reset_index(drop=True)
    (_BASE_DIR / "data").mkdir(exist_ok=True)
    df.to_csv(_BASE_DIR / "data" / "symbols_catalog.csv", index=False)
    return df

def _fetch_history_via_chart(symbol: str, period: str = "max", interval: str = "1d") -> pd.DataFrame | None:
    """Fallback downloader using Yahoo chart API via curl with browser-like headers.
    Returns OHLCV (+ Adj Close if available) DataFrame or None.
    """
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={period}&interval={interval}"
        headers = [
            "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "-H", "Accept: application/json, text/plain, */*",
            "-H", f"Referer: https://finance.yahoo.com/quote/{symbol}",
            "-H", "Accept-Language: en-US,en;q=0.9",
        ]
        out = _sp.check_output(["curl", "-s", *headers, url], timeout=20)
        if not out:
            return None
        data = _json.loads(out.decode("utf-8", errors="ignore"))
        result = (data or {}).get("chart", {}).get("result")
        if not result:
            return None
        res = result[0]
        ts = res.get("timestamp") or []
        if not ts:
            return None
        import numpy as _np
        idx = pd.to_datetime(_np.array(ts, dtype="int64"), unit="s", utc=True).tz_convert(None)
        ind = res.get("indicators", {})
        quote = (ind.get("quote") or [{}])[0]
        open_ = quote.get("open")
        high_ = quote.get("high")
        low_ = quote.get("low")
        close_ = quote.get("close")
        vol_ = quote.get("volume")
        df = pd.DataFrame({
            "Open": open_,
            "High": high_,
            "Low": low_,
            "Close": close_,
            "Volume": vol_,
        }, index=idx)
        adj = (ind.get("adjclose") or [{}])[0].get("adjclose")
        if adj is not None:
            df["Adj Close"] = adj
        # Clean
        df = df.dropna(how="all")
        if "Adj Close" in df.columns:
            cols = ["Open","High","Low","Close","Adj Close","Volume"]
            df = df[[c for c in cols if c in df.columns]]
        return df
    except Exception:
        return None

def update_all_daily_data(universe: dict, pause: float = 0.6, symbols: list[str] | None = None, lookback_days: int = 0) -> pd.DataFrame:
    if symbols is None:
        symbols = list_primary_symbols(universe)
    results = []
    for sym in symbols:
        try:
            path, added, changed_recent = update_symbol_csv(sym, pause=pause, lookback_days=lookback_days)
            updated = (added > 0) or changed_recent
            if added > 0 and changed_recent:
                reason = f"appended {added} & revised_recent(lb={lookback_days})"
            elif added > 0:
                reason = f"appended {added} (lb={lookback_days})"
            elif changed_recent:
                reason = f"revised_recent(lb={lookback_days})"
            else:
                reason = f"up_to_date(lb={lookback_days})"
            results.append({
                "symbol": sym,
                "file": str(path),
                "added": added,
                "updated": updated,
                "status": "ok" if updated else "no_change",
                "reason": reason,
            })
        except Exception as e:
            results.append({
                "symbol": sym,
                "file": str(_csv_path_for(sym)),
                "added": 0,
                "updated": False,
                "status": "error",
                "reason": str(e)[:200],
            })
        time.sleep(pause)
    df = pd.DataFrame(results)
    # Mark execution time for visibility in CI even when no rows are added
    try:
        ts = datetime.now(_ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    except Exception:
        ts = datetime.now().isoformat()
    df["run_at"] = ts
    df.to_csv(_BASE_DIR / "data" / "update_summary.csv", index=False)
    return df

# ============================
# Valuation Snapshot Cache (New)
# ============================

_VAL_DIR = _BASE_DIR / "data" / "valuations"
_VAL_DIR.mkdir(parents=True, exist_ok=True)

_VALUATION_FIELDS = [
    # Limit to the 3 consistently available fields
    "trailingPE",
    "priceToBook",
    "trailingAnnualDividendYield",
]

def _valuation_csv_path(symbol: str) -> _Path:
    return _VAL_DIR / f"{_sanitize_symbol(symbol)}.csv"

def _has_price_for_date(symbol: str, date_iso: str) -> bool:
    """Return True if daily price CSV for symbol has a row for date_iso (YYYY-MM-DD)."""
    try:
        p = _csv_path_for(symbol)
        if not p.exists():
            return False
        df = pd.read_csv(p)
        if "Date" not in df.columns or df.empty:
            return False
        s = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.date.astype(str)
        return (s == date_iso).any()
    except Exception:
        return False

def _latest_price_date(symbol: str) -> tuple[str | None, int | None]:
    """Return (latest_price_date_iso, days_ago) from the symbol's daily CSV.
    If file missing or no dates, returns (None, None).
    """
    try:
        p = _csv_path_for(symbol)
        if not p.exists():
            return None, None
        df = pd.read_csv(p)
        if "Date" not in df.columns or df.empty:
            return None, None
        s = pd.to_datetime(df["Date"], errors="coerce", utc=True).dt.date
        s = s.dropna()
        if s.empty:
            return None, None
        last = s.max()
        from datetime import datetime as _dt, timezone as _tz
        today = _dt.now(_tz.utc).date()
        return last.isoformat(), (today - last).days
    except Exception:
        return None, None

def fetch_valuation_snapshot(symbol: str) -> dict | None:
    """
    Fetch snapshot valuation metrics for a symbol using yfinance .info.
    Returns dict with available fields; None if unavailable.
    """
    try:
        t = yf.Ticker(symbol)
        info = t.info
        if not info or not isinstance(info, dict):
            return None
        row = {k: info.get(k) for k in _VALUATION_FIELDS}
        # If all values are None, treat as unavailable
        if all(v is None for v in row.values()):
            return None
        # Add metadata
        row.update({
            "symbol": symbol,
            "currency": info.get("currency"),
            "quoteType": info.get("quoteType"),
        })
        return row
    except Exception:
        return None

# (list_primary_symbols_for_valuation removed; use list_primary_symbols instead.)

def update_valuation_csv(symbol: str) -> tuple[_Path, bool]:
    """
    Append today's valuation snapshot for symbol if not already present.
    Returns (path, appended?)
    """
    path = _valuation_csv_path(symbol)
    from datetime import datetime as _dt
    # Use the latest available price bar date instead of strict UTC today
    eff_date, _age = _latest_price_date(symbol)
    if not eff_date:
        return path, False
    snap = fetch_valuation_snapshot(symbol)
    if snap is None:
        return path, False
    df_new = pd.DataFrame([{**{"Date": eff_date}, **snap}])
    if path.exists():
        try:
            df = pd.read_csv(path)
            if (df["Date"] == eff_date).any():
                return path, False
            df = pd.concat([df, df_new], ignore_index=True)
        except Exception:
            df = df_new
    else:
        df = df_new
    tmp = path.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path, True

def _batch_fetch_quote(symbols: list[str]) -> dict:
    """
    Fetch quote fields in a single call for multiple symbols using Yahoo v7 API via curl.
    Returns mapping: symbol -> dict(fields) for available results.
    Only a light set of headers to mimic a browser.
    """
    if not symbols:
        return {}
    import urllib.parse as _up
    # URL-encode symbols for query param (handles '^')
    joined = ",".join(symbols)
    # Request specific fields to ensure availability
    fields = [
        "trailingPE",
        "priceToBook",
        "trailingAnnualDividendYield",
        "dividendYield",
        "trailingAnnualDividendRate",
        "bookValue",
        "epsTrailingTwelveMonths",
        "regularMarketPrice",
        "currency",
        "quoteType",
    ]
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={_up.quote(joined)}"
        f"&fields={_up.quote(','.join(fields))}"
    )
    headers = [
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "-H", "Accept: application/json, text/plain, */*",
        "-H", "Accept-Language: en-US,en;q=0.9",
        "-H", "Referer: https://finance.yahoo.com/",
    ]
    try:
        out = _sp.check_output(["curl", "-s", *headers, url], timeout=25)
        data = _json.loads(out.decode("utf-8", errors="ignore"))
        results = (((data or {}).get("quoteResponse") or {}).get("result")) or []
        out_map: dict[str, dict] = {}
        for item in results:
            sym = item.get("symbol")
            if not sym:
                continue
            # Base: direct fields
            row = {k: item.get(k) for k in _VALUATION_FIELDS}
            # Derive dividend yield if missing
            if row.get("trailingAnnualDividendYield") in (None, "None"):
                dy = item.get("dividendYield")
                if dy in (None, "None"):
                    rate = item.get("trailingAnnualDividendRate")
                    price = item.get("regularMarketPrice") or 0
                    if rate not in (None, "None") and price:
                        try:
                            dy = float(rate) / float(price)
                        except Exception:
                            dy = None
                row["trailingAnnualDividendYield"] = dy
            # Derive P/B if missing
            if row.get("priceToBook") in (None, "None"):
                bv = item.get("bookValue")
                price = item.get("regularMarketPrice")
                if bv not in (None, "None", 0) and price not in (None, "None"):
                    try:
                        row["priceToBook"] = float(price) / float(bv)
                    except Exception:
                        pass
            # Derive P/E if missing
            if row.get("trailingPE") in (None, "None"):
                eps = item.get("epsTrailingTwelveMonths")
                price = item.get("regularMarketPrice")
                if eps not in (None, "None", 0) and price not in (None, "None"):
                    try:
                        pe = float(price) / float(eps)
                        # Avoid absurd values due to tiny EPS
                        if np.isfinite(pe) and pe > 0:
                            row["trailingPE"] = pe
                    except Exception:
                        pass
            row.update({
                "symbol": sym,
                "currency": item.get("currency"),
                "quoteType": item.get("quoteType"),
            })
            out_map[sym] = row
        return out_map
    except Exception:
        return {}

def _append_valuation_row(symbol: str, row: dict) -> tuple[_Path, bool]:
    """Append a single day's row to the symbol's valuation CSV if not present.

    Behavior tweak: if there's no price bar today, we still append when metrics
    changed versus the latest stored row. This allows weekend updates only when
    they are meaningful, avoiding duplicate weekend rows.
    """
    path = _valuation_csv_path(symbol)
    from datetime import datetime as _dt
    eff_date, _age = _latest_price_date(symbol)
    if not eff_date:
        # Without a recent price bar date, skip creating a row
        return _valuation_csv_path(symbol), False
    df_new = pd.DataFrame([{**{"Date": eff_date}, **row}])
    # Load existing for duplicate checks
    df = None
    if path.exists():
        try:
            df = pd.read_csv(path)
        except Exception:
            df = None
    if df is not None and not df.empty:
        if (df.get("Date") == eff_date).any():
            return path, False
        # Compare against last stored row for metric equality
        last = df.iloc[-1].to_dict()
        import numpy as _np
        def _norm(v):
            try:
                fv = float(v)
                if _np.isnan(fv):
                    return None
                return fv
            except Exception:
                return None
        changed = False
        for k in _VALUATION_FIELDS:
            if k in row:
                a = _norm(row.get(k))
                b = _norm(last.get(k))
                if a is None and b is None:
                    continue
                if a is None or b is None:
                    changed = True; break
                if not _np.isclose(a, b, rtol=1e-9, atol=1e-12):
                    changed = True; break
        # If metrics unchanged versus latest stored row, skip appending
        if not changed:
            # Allow override to append even if metrics unchanged
            try:
                import os as _os
                _always = _os.environ.get("VALUATION_ALWAYS_APPEND", "0").lower() in {"1","true","yes","on"}
            except Exception:
                _always = False
            if not _always:
                return path, False
        # else proceed to append
        try:
            df = pd.concat([df, df_new], ignore_index=True)
        except Exception:
            df = df_new
    else:
        # No prior file OR unreadable -> write new row regardless of price bar
        df = df_new
    tmp = path.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)
    return path, True

# ============================
# KRX Integration (pykrx)
# ============================

def update_krx_indices(run_backfill: bool = True,
                       price_mode: str = "full",
                       price_years: int = 3,
                       pause: float = 0.0) -> pd.DataFrame | None:
    """Update KRX index OHLCV and valuations using krx_data module.

    - run_backfill: if True, run valuation backfill; else append_today.
    - price_mode: 'full' for full backfill; 'quick' limits initial backfill to recent years.
    - price_years: used when price_mode='quick'.
    Returns batch summary DataFrame or None on import failure.
    """
    try:
        # Prefer normal import if package is resolvable
        try:
            from global_universe import krx_data as _krx
        except Exception:
            import importlib.util, sys as _sys
            _spec = importlib.util.spec_from_file_location("krx_data", (_BASE_DIR / "krx_data.py"))
            _krx = importlib.util.module_from_spec(_spec)
            assert _spec and _spec.loader
            _spec.loader.exec_module(_krx)
        # Control price mode via env variables understood by krx_data
        import os as _os
        _os.environ["KRX_PRICE_MODE"] = price_mode
        if price_mode == "quick":
            _os.environ["KRX_PRICE_YEARS"] = str(int(price_years))
        mode = "backfill" if run_backfill else "append_today"
        df = _krx.batch_update_indices(_krx.KRX_TEST_INDICES, valuation_mode=mode)
        # Save a separate summary file
        out_csv = _BASE_DIR / "data" / "krx_batch_summary_from_world_indices.csv"
        df.to_csv(out_csv, index=False)
        return df
    except Exception:
        return None

def update_all_valuations(
    universe: dict,
    pause: float = 0.2,
    symbols: list[str] | None = None,
    max_symbols: int | None = None,
    mode: str = "batch_quote",
    chunk: int = 20,
    info_fallback: bool = True,
    max_info_calls: int | None = None,
) -> pd.DataFrame:
    """
    Build/update daily valuation snapshots for each asset (one symbol per asset).
    Preference: index; if index has no valuation fields, fallback to ETF.
    No use of alternatives and no historical backfill (snapshot only).
    """
    rows = []
    # Helper to test if any valuation metric is present
    def _has_metrics(d: dict | None) -> bool:
        if not isinstance(d, dict):
            return False
        return any(d.get(k) not in (None, "None") for k in _VALUATION_FIELDS)
    from datetime import datetime as _dt
    today = _dt.utcnow().date().isoformat()
    # Build the traversal list with optional filtering
    tasks = []
    for country, cdata in universe.items():
        for section in ("sectors", "factors"):
            for name, asset in cdata.get(section, {}).items():
                chosen = asset.get("index") or asset.get("etf")
                if symbols and chosen not in symbols:
                    continue
                tasks.append((country, section, name, asset, chosen))

    if max_symbols is not None:
        tasks = tasks[:max_symbols]

    if mode == "batch_quote":
        # 1) Primary fetch in chunks
        prim_syms = [t[4] for t in tasks if t[4]]
        prim_map: dict[str, dict] = {}
        for i in range(0, len(prim_syms), max(1, chunk)):
            batch = prim_syms[i:i+chunk]
            prim_map.update(_batch_fetch_quote(batch))
            time.sleep(pause)

        # 2) Build full fallback candidate lists per task (ETF + alternatives + index when applicable)
        fb_lists: list[list[str]] = []
        fb_set: list[str] = []
        for (_, _, _, asset, chosen) in tasks:
            cands: list[str] = []
            idx_sym = asset.get("index")
            etf_sym = asset.get("etf")
            alts = [a for a in (asset.get("alternatives") or []) if a]
            if chosen == idx_sym:
                if etf_sym:
                    cands.append(etf_sym)
                cands.extend(alts)
            else:
                # chosen is ETF or alternative; try other ETFs/alternatives first, then index
                # Keep order but skip the chosen symbol itself
                for a in ([etf_sym] if etf_sym else []) + alts:
                    if a and a != chosen and a not in cands:
                        cands.append(a)
                if idx_sym:
                    cands.append(idx_sym)
            fb_lists.append(cands)
            for s in cands:
                if s not in fb_set:
                    fb_set.append(s)

        fb_map: dict[str, dict] = {}
        if fb_set:
            for i in range(0, len(fb_set), max(1, chunk)):
                batch = fb_set[i:i+chunk]
                fb_map.update(_batch_fetch_quote(batch))
                time.sleep(pause)

        # 3) Write CSVs and build summary rows (with optional info fallback)
        info_calls = 0
        for ti, (country, section, name, asset, chosen) in enumerate(tasks):
            used_fallback = False
            path = _valuation_csv_path(chosen or "")
            updated = False
            used_symbol = chosen
            reason = None
            snap = prim_map.get(chosen)
            if not _has_metrics(snap):
                # try broader fallback list (ETF/alternatives/index depending on chosen)
                for sym in fb_lists[ti]:
                    alt = fb_map.get(sym)
                    if _has_metrics(alt):
                        path, updated = _append_valuation_row(sym, alt)
                        used_fallback = True
                        used_symbol = sym
                        break
            if not used_fallback and _has_metrics(snap):
                path, updated = _append_valuation_row(chosen, snap)
                used_symbol = chosen

            # Per-symbol info fallback (quoteSummary) if still not updated
            if not updated and info_fallback and (max_info_calls is None or info_calls < max_info_calls):
                # Try chosen, then ETF, then alternatives in order
                cand_syms = [s for s in [chosen, asset.get("etf"), *(asset.get("alternatives") or []), asset.get("index")] if s]
                for sym in cand_syms:
                    row = fetch_valuation_snapshot(sym)
                    info_calls += 1
                    if _has_metrics(row):
                        path, updated = _append_valuation_row(sym, row)
                        used_fallback = used_fallback or (sym != chosen)
                        used_symbol = sym
                        time.sleep(pause)
                        break
            # Determine status and reason
            status = "ok" if updated else "no_change"
            if not updated:
                # Check specific reasons
                eff_date, _age = _latest_price_date(used_symbol or chosen)
                has_price_bar = bool(eff_date)
                # Check if valuation file already has the effective price date row
                has_eff_row = False
                try:
                    if path and isinstance(path, _Path) and path.exists():
                        df_existing = pd.read_csv(path)
                        has_eff_row = ("Date" in df_existing.columns) and (df_existing["Date"] == (eff_date or "")).any()
                except Exception:
                    pass
                if has_eff_row:
                    reason = "already_has_effective_date"
                elif not has_price_bar:
                    reason = "no_recent_price_bar"
                else:
                    # No metrics found on all sources
                    primary_ok = _has_metrics(snap)
                    any_fb_ok = any(_has_metrics(fb_map.get(s)) for s in fb_lists[ti])
                    reason = "no_valuation_fields" if not (primary_ok or any_fb_ok) else "unknown_or_write_skipped"
                # If truly no data captured at all, mark status accordingly
                if not has_eff_row:
                    status = "no_data"

            rows.append({
                "country": country,
                "category": section,
                "name": name,
                "primary": chosen,
                "fallback_to_etf": used_fallback,
                "used_symbol": used_symbol,
                "file": str(path) if isinstance(path, _Path) else str(path),
                "updated": updated,
                "status": status,
                "reason": reason,
            })
    else:
        # Legacy per-symbol yfinance .info mode
        for (country, section, name, asset, chosen) in tasks:
            used_fallback = False
            path, updated = _valuation_csv_path(""), False  # init
            reason = None
            if chosen:
                path, updated = update_valuation_csv(chosen)
                if not updated and asset.get("index") and asset.get("index") == chosen:
                    # fallback to ETF/alternative if index had no valuation data today
                    fb = asset.get("etf")
                    if not fb:
                        alts = asset.get("alternatives") or []
                        fb = alts[0] if alts else None
                    if fb:
                        path, updated = update_valuation_csv(fb)
                        used_fallback = True
            # Determine status
            status = "ok" if updated else "no_change"
            if not updated:
                # Reasoning
                eff_date, _age = _latest_price_date(chosen)
                has_price_bar = bool(eff_date)
                # Check if valuation file already has the effective date row
                has_eff_row = False
                try:
                    if path and isinstance(path, _Path) and path.exists():
                        df_existing = pd.read_csv(path)
                        has_eff_row = ("Date" in df_existing.columns) and (df_existing["Date"] == (eff_date or "")).any()
                except Exception:
                    pass
                if has_eff_row:
                    reason = "already_has_effective_date"
                elif not has_price_bar:
                    reason = "no_recent_price_bar"
                else:
                    reason = "no_valuation_fields"
                if not has_eff_row:
                    status = "no_data"

            rows.append({
                "country": country,
                "category": section,
                "name": name,
                "primary": chosen,
                "fallback_to_etf": used_fallback,
                "file": str(path) if isinstance(path, _Path) else str(path),
                "updated": updated,
                "status": status,
                "reason": reason,
            })
            time.sleep(pause)
    df = pd.DataFrame(rows)
    # Mark execution time for visibility in CI
    try:
        ts = datetime.now(_ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    except Exception:
        ts = datetime.now().isoformat()
    df["run_at"] = ts
    df.to_csv(_BASE_DIR / "data" / "valuations_update_summary.csv", index=False)
    return df

# Run data updater if script is executed
if __name__ == "__main__":
    try:
        import os as _os
        # Optional sanitization pass for daily CSVs
        _sanitize = _os.environ.get("SANITIZE_DAILY", "0").lower() in {"1","true","yes","on"}
        _sanitize_only = _os.environ.get("SANITIZE_ONLY", "0").lower() in {"1","true","yes","on"}
        if _sanitize or _sanitize_only:
            print("Sanitizing daily CSV files (removing merge markers, dedup headers)...")
            ssum = sanitize_all_daily_csvs()
            print("Sanitization complete. Summary saved to data/daily_sanitization_summary.csv")
            # If sanitize-only, exit before doing network work
            if _sanitize_only:
                raise SystemExit(0)
        # Determine scope and mode
        price_scope = _os.environ.get("PRICE_SCOPE", "primary").lower()  # 'primary' | 'all'
        price_mode = _os.environ.get("PRICE_MODE", "append").lower()    # 'append' | 'backfill'
        _is_ci = _os.environ.get("GITHUB_ACTIONS") == "true"
        price_pause = float(_os.environ.get("PRICE_PAUSE", "0.6" if _is_ci else "0.3"))
        price_lookback = int(_os.environ.get("PRICE_LOOKBACK", "7"))

        # Build catalog for visibility
        build_symbols_catalog(investment_universe, primary_only=(price_scope != "all"))
        if price_scope == "all":
            syms = list_all_symbols(investment_universe)
        else:
            syms = list_primary_symbols(investment_universe)
        # Optional manual symbol list override
        _syms_env = _os.environ.get("PRICE_SYMBOLS")
        if _syms_env:
            syms = [s.strip() for s in _syms_env.split(',') if s.strip()]
        # Allow optional limit for testing speed
        _max = _os.environ.get("MAX_SYMBOLS")
        if _max:
            syms = syms[:int(_max)]

        print(f"Price update scope={price_scope} mode={price_mode} lb={price_lookback} symbols={len(syms)}")
        if price_mode == "backfill":
            summary = backfill_all_prices(investment_universe, pause=price_pause, symbols=syms)
            print("Backfill complete. Summary saved to data/prices_backfill_summary.csv")
        else:
            summary = update_all_daily_data(investment_universe, pause=price_pause, symbols=syms, lookback_days=price_lookback)
            print("Incremental price update complete. Summary saved to data/update_summary.csv")

        # Valuation snapshots (snapshot only, daily append)
        skip_vals = _os.environ.get("SKIP_VALUATIONS", "false").lower() in {"1","true","yes","on"}
        if not skip_vals:
            print("Updating valuation snapshots (primary symbols, with ETF fallback)...")
            val_pause = float(_os.environ.get("VALUATION_PAUSE", "1.0" if _is_ci else "0.2"))
            # Optional limiting in CI to avoid 429
            _max_val = _os.environ.get("MAX_VAL_SYMBOLS")
            _val_syms_env = _os.environ.get("VAL_SYMBOLS")  # comma-separated list, optional
            _val_symbols = None
            if _val_syms_env:
                _val_symbols = [s.strip() for s in _val_syms_env.split(",") if s.strip()]
            _mode = _os.environ.get("VALUATION_FETCH_MODE", "batch_quote")
            _chunk = int(_os.environ.get("VALUATION_CHUNK", "20"))
            _info_fallback = (_os.environ.get("VALUATION_INFO_FALLBACK", "1").lower() in {"1","true","yes","on"})
            _max_info_calls = _os.environ.get("MAX_INFO_CALLS")
            vsummary = update_all_valuations(
                investment_universe,
                pause=val_pause,
                symbols=_val_symbols,
                max_symbols=int(_max_val) if _max_val else None,
                mode=_mode,
                chunk=_chunk,
                info_fallback=_info_fallback,
                max_info_calls=int(_max_info_calls) if _max_info_calls else None,
            )
            print("Valuation update complete. Saved to data/valuations_update_summary.csv")
            print(vsummary.head())
        else:
            print("Skipping valuation snapshots (set FORCE_VALUATIONS=1 to override).")

        # KRX indices via pykrx (prices + valuations)
        _krx_run = _os.environ.get("INCLUDE_KRX", "1").lower() in {"1","true","yes","on"}
        if _krx_run:
            print("Updating KRX index prices and valuations (via pykrx)...")
            _krx_backfill = _os.environ.get("KRX_VAL_MODE", "backfill").lower() == "backfill"
            _krx_price_mode = _os.environ.get("KRX_PRICE_MODE", "full")
            _krx_price_years = int(_os.environ.get("KRX_PRICE_YEARS", "3"))
            ksum = update_krx_indices(run_backfill=_krx_backfill,
                                      price_mode=_krx_price_mode,
                                      price_years=_krx_price_years)
            if ksum is not None:
                print("KRX update complete. Saved to data/krx_batch_summary_from_world_indices.csv")
            else:
                print("KRX update skipped (import or runtime error)")
    except Exception as e:
        print(f"Error during update: {e}")
