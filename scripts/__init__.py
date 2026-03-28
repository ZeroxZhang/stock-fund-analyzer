"""
Stock & Fund Analyzer - Data Collection Scripts
股票基金综合分析框架 - 数据采集参考实现

This module provides reference implementations for fetching financial data
from various sources including yfinance, akshare, and public APIs.

Dependencies:
    pip install yfinance akshare pandas requests
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta

# ============================================================================
# 美股数据采集 (US Stock Data)
# ============================================================================

def fetch_stock_info(ticker: str) -> Dict[str, Any]:
    """
    获取美股基础信息、估值指标和机构预期
    
    Args:
        ticker: 股票代码 (e.g., "AAPL", "TSLA", "NVDA")
    
    Returns:
        dict: 包含基础信息、估值指标、机构预期的字典
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("Please install yfinance: pip install yfinance")
    
    stock = yf.Ticker(ticker)
    info = stock.info
    
    return {
        "ticker": ticker,
        "company_name": info.get("longName", "N/A"),
        "sector": info.get("sector", "N/A"),
        "industry": info.get("industry", "N/A"),
        "current_price": info.get("currentPrice", info.get("regularMarketPrice", "N/A")),
        "market_cap": info.get("marketCap", "N/A"),
        "pe_ttm": info.get("trailingPE", "N/A"),
        "pe_forward": info.get("forwardPE", "N/A"),
        "ps_ttm": info.get("priceToSalesTrailing12Months", "N/A"),
        "pb": info.get("priceToBook", "N/A"),
        "peg": info.get("pegRatio", "N/A"),
        "dividend_yield": info.get("dividendYield", "N/A"),
        "beta": info.get("beta", "N/A"),
        "52_week_high": info.get("fiftyTwoWeekHigh", "N/A"),
        "52_week_low": info.get("fiftyTwoWeekLow", "N/A"),
        "avg_volume": info.get("averageVolume", "N/A"),
        "shares_outstanding": info.get("sharesOutstanding", "N/A"),
        "float_shares": info.get("floatShares", "N/A"),
        "institutional_ownership": info.get("institutionOwnership", "N/A"),
        "recommendation": info.get("recommendationKey", "N/A"),
        "target_price_mean": info.get("targetMeanPrice", "N/A"),
        "target_price_median": info.get("targetMedianPrice", "N/A"),
        "analyst_count": info.get("numberOfAnalystOpinions", "N/A"),
        "data_source": "yfinance",
        "fetch_time": datetime.now().isoformat()
    }


def fetch_stock_financials(ticker: str, periods: int = 4) -> Dict[str, Any]:
    """
    获取股票财务数据（营收、利润、现金流、ROE/ROIC等）
    
    Args:
        ticker: 股票代码
        periods: 获取的财报期数（默认4期，即最近4个季度/年度）
    
    Returns:
        dict: 包含财务报表关键数据的字典
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("Please install yfinance: pip install yfinance")
    
    stock = yf.Ticker(ticker)
    
    # 获取财务报表
    income_stmt = stock.financials
    balance_sheet = stock.balance_sheet
    cash_flow = stock.cashflow
    
    # 获取关键财务指标
    try:
        metrics = {
            "ticker": ticker,
            "financials": {
                "revenue": income_stmt.loc["Total Revenue"].to_dict() if "Total Revenue" in income_stmt.index else {},
                "gross_profit": income_stmt.loc["Gross Profit"].to_dict() if "Gross Profit" in income_stmt.index else {},
                "operating_income": income_stmt.loc["Operating Income"].to_dict() if "Operating Income" in income_stmt.index else {},
                "net_income": income_stmt.loc["Net Income"].to_dict() if "Net Income" in income_stmt.index else {},
                "eps": income_stmt.loc["Basic EPS"].to_dict() if "Basic EPS" in income_stmt.index else {},
            },
            "balance_sheet": {
                "total_assets": balance_sheet.loc["Total Assets"].to_dict() if "Total Assets" in balance_sheet.index else {},
                "total_liabilities": balance_sheet.loc["Total Liabilities Net Minority Interest"].to_dict() if "Total Liabilities Net Minority Interest" in balance_sheet.index else {},
                "stockholders_equity": balance_sheet.loc["Stockholders Equity"].to_dict() if "Stockholders Equity" in balance_sheet.index else {},
                "cash": balance_sheet.loc["Cash And Cash Equivalents"].to_dict() if "Cash And Cash Equivalents" in balance_sheet.index else {},
                "total_debt": balance_sheet.loc["Total Debt"].to_dict() if "Total Debt" in balance_sheet.index else {},
            },
            "cash_flow": {
                "operating_cash_flow": cash_flow.loc["Operating Cash Flow"].to_dict() if "Operating Cash Flow" in cash_flow.index else {},
                "capital_expenditure": cash_flow.loc["Capital Expenditure"].to_dict() if "Capital Expenditure" in cash_flow.index else {},
                "free_cash_flow": cash_flow.loc["Free Cash Flow"].to_dict() if "Free Cash Flow" in cash_flow.index else {},
            },
            "data_source": "yfinance",
            "fetch_time": datetime.now().isoformat()
        }
        
        # 计算关键比率
        if metrics["financials"]["revenue"] and metrics["financials"]["net_income"]:
            latest_revenue = list(metrics["financials"]["revenue"].values())[0]
            latest_net_income = list(metrics["financials"]["net_income"].values())[0]
            if latest_revenue and latest_revenue != 0:
                metrics["ratios"] = {
                    "net_margin": latest_net_income / latest_revenue if latest_revenue else None,
                }
        
        return metrics
    except Exception as e:
        return {"error": str(e), "ticker": ticker}


def fetch_price_history(
    ticker: str, 
    period: str = "1y", 
    interval: str = "1d"
) -> pd.DataFrame:
    """
    获取历史价格数据（OHLCV）用于技术分析
    
    Args:
        ticker: 股票代码
        period: 时间周期 ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
        interval: K线间隔 ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")
    
    Returns:
        DataFrame: 包含 Open, High, Low, Close, Volume 的历史价格数据
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("Please install yfinance: pip install yfinance")
    
    stock = yf.Ticker(ticker)
    df = stock.history(period=period, interval=interval)
    
    df["Ticker"] = ticker
    df["FetchTime"] = datetime.now().isoformat()
    
    return df


# ============================================================================
# ETF 数据采集 (ETF Data)
# ============================================================================

def fetch_etf_info(ticker: str) -> Dict[str, Any]:
    """
    获取ETF基础信息、费率、收益等
    
    Args:
        ticker: ETF代码 (e.g., "QQQ", "SPY", "VTI")
    
    Returns:
        dict: 包含ETF基础信息的字典
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("Please install yfinance: pip install yfinance")
    
    etf = yf.Ticker(ticker)
    info = etf.info
    
    return {
        "ticker": ticker,
        "name": info.get("longName", "N/A"),
        "category": info.get("category", "N/A"),
        "current_price": info.get("currentPrice", info.get("regularMarketPrice", "N/A")),
        "nav": info.get("navPrice", "N/A"),
        "expense_ratio": info.get("annualReportExpenseRatio", "N/A"),
        "yield": info.get("yield", "N/A"),
        "ytd_return": info.get("ytdReturn", "N/A"),
        "three_year_return": info.get("threeYearAverageReturn", "N/A"),
        "five_year_return": info.get("fiveYearAverageReturn", "N/A"),
        "assets_under_management": info.get("totalAssets", "N/A"),
        "avg_volume": info.get("averageVolume", "N/A"),
        "pe_ratio": info.get("priceToBook", "N/A"),
        "pb_ratio": info.get("priceToBook", "N/A"),
        "data_source": "yfinance",
        "fetch_time": datetime.now().isoformat()
    }


def fetch_etf_holdings(ticker: str, top_n: int = 10) -> List[Dict[str, Any]]:
    """
    获取ETF前十大持仓
    
    Args:
        ticker: ETF代码
        top_n: 返回的持仓数量
    
    Returns:
        list: 持仓列表
    """
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("Please install yfinance: pip install yfinance")
    
    etf = yf.Ticker(ticker)
    
    try:
        holdings = etf.get_top_holdings()
        if holdings is not None and not holdings.empty:
            result = []
            for idx, row in holdings.head(top_n).iterrows():
                result.append({
                    "symbol": row.get("Symbol", str(idx)),
                    "name": row.get("Name", "N/A"),
                    "weight": row.get("% Assets", "N/A"),
                })
            return result
    except Exception:
        pass
    
    return [{"note": "Holdings data not available for this ETF", "ticker": ticker}]


# ============================================================================
# A股数据采集 (A-Share Data)
# ============================================================================

def fetch_cn_stock_info(symbol: str) -> Dict[str, Any]:
    """
    获取A股基础信息（使用akshare）
    
    Args:
        symbol: 股票代码 (e.g., "000001" for 平安银行)
    
    Returns:
        dict: 包含A股基础信息的字典
    """
    try:
        import akshare as ak
    except ImportError:
        raise ImportError("Please install akshare: pip install akshare")
    
    try:
        # 获取实时行情
        df = ak.stock_zh_a_spot_em()
        stock_data = df[df["代码"] == symbol]
        
        if stock_data.empty:
            return {"error": f"Stock {symbol} not found", "symbol": symbol}
        
        row = stock_data.iloc[0]
        
        return {
            "symbol": symbol,
            "name": row.get("名称", "N/A"),
            "current_price": row.get("最新价", "N/A"),
            "change_pct": row.get("涨跌幅", "N/A"),
            "change_amount": row.get("涨跌额", "N/A"),
            "volume": row.get("成交量", "N/A"),
            "amount": row.get("成交额", "N/A"),
            "high": row.get("最高", "N/A"),
            "low": row.get("最低", "N/A"),
            "open": row.get("今开", "N/A"),
            "prev_close": row.get("昨收", "N/A"),
            "pe_ratio": row.get("市盈率-动态", "N/A"),
            "pb_ratio": row.get("市净率", "N/A"),
            "total_market_value": row.get("总市值", "N/A"),
            "circulating_market_value": row.get("流通市值", "N/A"),
            "data_source": "akshare",
            "fetch_time": datetime.now().isoformat()
        }
    except Exception as e:
        return {"error": str(e), "symbol": symbol}


def fetch_cn_money_flow(symbol: str) -> Dict[str, Any]:
    """
    获取A股资金流向数据（北向资金、主力资金、融资融券）
    
    Args:
        symbol: 股票代码
    
    Returns:
        dict: 资金流向数据
    """
    try:
        import akshare as ak
    except ImportError:
        raise ImportError("Please install akshare: pip install akshare")
    
    result = {
        "symbol": symbol,
        "data_source": "akshare",
        "fetch_time": datetime.now().isoformat()
    }
    
    try:
        # 个股资金流向
        df = ak.stock_individual_fund_flow(stock=symbol, market="sh" if symbol.startswith("6") else "sz")
        if df is not None and not df.empty:
            result["individual_flow"] = df.to_dict("records")[-5:]  # 最近5天
    except Exception as e:
        result["individual_flow_error"] = str(e)
    
    try:
        # 北向资金
        df = ak.stock_hsgt_north_net_flow_in_em()
        if df is not None and not df.empty:
            result["north_bound_flow"] = df.to_dict("records")[-5:]  # 最近5天
    except Exception as e:
        result["north_bound_flow_error"] = str(e)
    
    return result


# ============================================================================
# 宏观与行业数据 (Macro & Industry Data)
# ============================================================================

def fetch_macro_indicators() -> Dict[str, Any]:
    """
    获取宏观指标数据（利率、汇率等）
    
    Returns:
        dict: 宏观指标数据
    """
    result = {
        "data_source": "multiple",
        "fetch_time": datetime.now().isoformat()
    }
    
    # 美元指数 (通过 yfinance)
    try:
        import yfinance as yf
        dxy = yf.Ticker("DX-Y.NYB")
        dxy_info = dxy.info
        result["us_dollar_index"] = {
            "current": dxy_info.get("currentPrice", dxy_info.get("regularMarketPrice", "N/A")),
            "prev_close": dxy_info.get("previousClose", "N/A"),
        }
    except Exception:
        result["us_dollar_index"] = {"note": "Data not available"}
    
    # 美国国债收益率
    try:
        import yfinance as yf
        tnx = yf.Ticker("^TNX")  # 10年期国债收益率
        tnx_info = tnx.info
        result["us_10y_yield"] = {
            "current": tnx_info.get("currentPrice", tnx_info.get("regularMarketPrice", "N/A")),
        }
    except Exception:
        result["us_10y_yield"] = {"note": "Data not available"}
    
    # VIX 恐慌指数
    try:
        import yfinance as yf
        vix = yf.Ticker("^VIX")
        vix_info = vix.info
        result["vix"] = {
            "current": vix_info.get("currentPrice", vix_info.get("regularMarketPrice", "N/A")),
        }
    except Exception:
        result["vix"] = {"note": "Data not available"}
    
    return result


def fetch_industry_index() -> Dict[str, Any]:
    """
    获取申万行业指数数据
    
    Returns:
        dict: 行业指数数据
    """
    try:
        import akshare as ak
    except ImportError:
        raise ImportError("Please install akshare: pip install akshare")
    
    result = {
        "data_source": "akshare",
        "fetch_time": datetime.now().isoformat(),
        "industries": []
    }
    
    try:
        # 申万一级行业指数
        df = ak.sw_index_daily()
        if df is not None and not df.empty:
            # 获取最新数据
            latest = df.to_dict("records")
            result["industries"] = latest[:20]  # 返回前20个行业
    except Exception as e:
        result["error"] = str(e)
    
    return result


# ============================================================================
# 工具函数 (Utility Functions)
# ============================================================================

def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算常用技术指标
    
    Args:
        df: 包含 OHLCV 数据的 DataFrame
    
    Returns:
        DataFrame: 添加了技术指标的数据
    """
    import numpy as np
    
    df = df.copy()
    
    # 移动平均线
    df["MA5"] = df["Close"].rolling(window=5).mean()
    df["MA10"] = df["Close"].rolling(window=10).mean()
    df["MA20"] = df["Close"].rolling(window=20).mean()
    df["MA50"] = df["Close"].rolling(window=50).mean()
    df["MA200"] = df["Close"].rolling(window=200).mean()
    
    # RSI (14)
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df["RSI14"] = 100 - (100 / (1 + rs))
    
    # MACD
    exp12 = df["Close"].ewm(span=12, adjust=False).mean()
    exp26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = exp12 - exp26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Histogram"] = df["MACD"] - df["MACD_Signal"]
    
    # 布林带
    df["BB_Middle"] = df["Close"].rolling(window=20).mean()
    bb_std = df["Close"].rolling(window=20).std()
    df["BB_Upper"] = df["BB_Middle"] + (bb_std * 2)
    df["BB_Lower"] = df["BB_Middle"] - (bb_std * 2)
    
    # ATR (14)
    high_low = df["High"] - df["Low"]
    high_close = np.abs(df["High"] - df["Close"].shift())
    low_close = np.abs(df["Low"] - df["Close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(window=14).mean()
    
    return df


# ============================================================================
# 主函数示例 (Example Usage)
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Stock & Fund Analyzer - Data Collection Demo")
    print("=" * 60)
    
    # 示例：获取苹果股票信息
    print("\n[1] Fetching AAPL stock info...")
    aapl_info = fetch_stock_info("AAPL")
    print(f"Company: {aapl_info.get('company_name', 'N/A')}")
    print(f"Current Price: ${aapl_info.get('current_price', 'N/A')}")
    print(f"P/E (TTM): {aapl_info.get('pe_ttm', 'N/A')}")
    
    # 示例：获取历史价格
    print("\n[2] Fetching AAPL price history (last 30 days)...")
    aapl_history = fetch_price_history("AAPL", period="1mo")
    print(f"Records: {len(aapl_history)}")
    if not aapl_history.empty:
        print(aapl_history.tail())
    
    # 示例：计算技术指标
    print("\n[3] Calculating technical indicators...")
    aapl_with_indicators = calculate_technical_indicators(aapl_history)
    print(f"Columns: {list(aapl_with_indicators.columns)}")
    
    # 示例：获取ETF信息
    print("\n[4] Fetching QQQ ETF info...")
    qqq_info = fetch_etf_info("QQQ")
    print(f"Name: {qqq_info.get('name', 'N/A')}")
    print(f"Expense Ratio: {qqq_info.get('expense_ratio', 'N/A')}")
    
    # 示例：获取宏观指标
    print("\n[5] Fetching macro indicators...")
    macro = fetch_macro_indicators()
    print(f"USD Index: {macro.get('us_dollar_index', {}).get('current', 'N/A')}")
    print(f"VIX: {macro.get('vix', {}).get('current', 'N/A')}")
    
    print("\n" + "=" * 60)
    print("Demo completed!")
    print("=" * 60)
