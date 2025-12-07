import time
import pymysql
import requests
import numpy as np
import pandas as pd
import configparser

from FinMind.data import FinMindApi
from datetime import datetime, timedelta
from sqlalchemy.dialects.mysql import insert  
from sqlalchemy import create_engine, text, Table, Column, Integer, String, Float, BigInteger, Date, MetaData, UniqueConstraint

# 讀取設定檔
config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")


# 查詢該股票最後更新日期
def get_last_date(stock_id, mode):
    mode_table_mapping = {
        "stock_prices" : "stock_prices",
        "TAIEX" : "TW_index",
        "TPEx" : "TW_index",
        "margin_short" : "margin_short",
        "institutional_trades" : "institutional_trades",
        "month_revenue" : "month_revenue",
        "per" : "per",
        "financial_statements" : "financial_statements",
        "balance_sheet" : "balance_sheet",
        "cash_flow" : "cash_flow"
    }
    
    sql = text(f"SELECT MAX(date) FROM {mode_table_mapping[mode]} WHERE stock_id=:stock_id")
    with engine.connect() as conn:
        result = conn.execute(sql, {"stock_id": stock_id}).scalar()

    return result


# 建立資料庫table
metadata = MetaData()

stock_codes_table = Table(
    "stock_codes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("industry_category", String(255)),
    Column("stock_id", String(50), nullable=False),
    Column("stock_name", String(255)),
    Column("type", String(50)),
    Column("date", Date, nullable=True),
    UniqueConstraint("stock_id", name="unique_stock_id")
)

stock_prices_table = Table(
        "stock_prices",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),         
        Column("date", Date, nullable=False),
        Column("open", Float),
        Column("max", Float),                                    
        Column("min", Float),                                   
        Column("close", Float),
        Column("Trading_Volume", BigInteger),
        Column("Trading_money", BigInteger),
        Column("spread", Float),
        Column("Trading_turnover", BigInteger),
        UniqueConstraint("stock_id", "date", name="unique_stock_date")
    )
 
TW_index_table = Table(
        "TW_index",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("price", Float),
        UniqueConstraint("stock_id", "date", name="unique_index_date")
    )

margin_short_table = Table(
        "margin_short",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("date", Date, nullable=False),
        Column("stock_id", String(10), nullable=False),
        Column("MarginPurchaseBuy", BigInteger),
        Column("MarginPurchaseCashRepayment", BigInteger),
        Column("MarginPurchaseLimit", BigInteger),
        Column("MarginPurchaseSell", BigInteger),
        Column("MarginPurchaseTodayBalance", BigInteger),
        Column("MarginPurchaseYesterdayBalance", BigInteger),
        Column("Note", String(255)),
        Column("OffsetLoanAndShort", BigInteger),
        Column("ShortSaleBuy", BigInteger),
        Column("ShortSaleCashRepayment", BigInteger),
        Column("ShortSaleLimit", BigInteger),
        Column("ShortSaleSell", BigInteger),
        Column("ShortSaleTodayBalance", BigInteger),
        Column("ShortSaleYesterdayBalance", BigInteger),
        UniqueConstraint("stock_id", "date", name="unique_stock_date")
    )

institutional_trades_table = Table(
        "institutional_trades",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("name", String(50), nullable=False),  # 法人名稱
        Column("buy", BigInteger),
        Column("sell", BigInteger),
        UniqueConstraint("stock_id", "date", "name", name="unique_stock_date_name")
    )

month_revenue_table = Table(
        "month_revenue",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("country", String(20)),
        Column("revenue", BigInteger),
        Column("revenue_month", BigInteger),
        Column("revenue_year", BigInteger),
        UniqueConstraint("stock_id", "date", name="unique_stock_date")
    )

per_table = Table(
        "per",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("dividend_yield", Float),
        Column("PER", Float),
        Column("PBR", Float),
        UniqueConstraint("stock_id", "date", name="unique_stock_date")
    )

financial_statements_table = Table(
        "financial_statements",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("type", String(255)),      
        Column("value", BigInteger),     
        Column("origin_name", String(255)),
        UniqueConstraint("stock_id", "type", "date", name="unique_stock_type_date")
    )

balance_sheet_table = Table(
        "balance_sheet",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("type", String(255)),      
        Column("value", BigInteger),     
        Column("origin_name", String(255)),
        UniqueConstraint("stock_id", "type", "date", name="unique_stock_type_date")
    )

cash_flow_table = Table(
        "cash_flow",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("stock_id", String(10), nullable=False),
        Column("date", Date, nullable=False),
        Column("type", String(255)),      
        Column("value", BigInteger),     
        Column("origin_name", String(255)),
        UniqueConstraint("stock_id", "type", "date", name="unique_stock_type_date")
    )


def create_all_tables():
    metadata.create_all(engine)
    print("所有表格已建立完成（若已存在則跳過）")


# 抓取資料
def fetch_data(dataset, stock_id, start_date="2010-01-01"):
    df = api.get_data(
        dataset = dataset,
        data_id = stock_id,
        start_date = start_date
    )

    if df.empty:
        print(f"沒有抓到 {stock_id} {dataset}")
    else:
        print(f"{stock_id} {dataset} 抓取完成，共 {len(df)} 筆")
    
    return df


# 更新股票代碼
def fetch_stock_codes():
    df = api.get_data(
        dataset = "TaiwanStockInfo"
    )
    
    if df.empty:
        print(f"沒有抓到股票代碼")
    else:
        print(f"股票代碼抓取完成，共 {len(df)} 筆")
    
    return df


fetch_data_dict = {
    "stock_prices" : "TaiwanStockPrice",
    "TAIEX" : "TaiwanStockTotalReturnIndex",
    "TPEx" : "TaiwanStockTotalReturnIndex",
    "margin_short" : "TaiwanStockMarginPurchaseShortSale",
    "institutional_trades" : "TaiwanStockInstitutionalInvestorsBuySell",
    "month_revenue" : "TaiwanStockMonthRevenue",
    "per" : "TaiwanStockPER",
    "financial_statements" : "TaiwanStockFinancialStatements",
    "balance_sheet" : "TaiwanStockBalanceSheet",
    "cash_flow" : "TaiwanStockCashFlowsStatement"
}


# 更新資料進資料庫
# mode : stock_prices, TAIEX, TPEx, margin_short, institutional_trades, month_revenue, per, financial_statements ,balance_sheet, cash_flow
def update_stock_codes(table):
    df = fetch_stock_codes()
    df = df.replace("None", None)

    if df.empty:
        print("沒有抓到股票代碼，更新中止")
        return False

    # 先清空 table
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stock_codes"))

    # 全部重寫
    df.to_sql("stock_codes", engine, if_exists="append", index=False)

    print(f"股票代碼更新完成，共 {len(df)} 筆")
    return True


def update_daily_data(stock_id, mode, table):
    today = datetime.today().date()
    last_date = get_last_date(stock_id, mode)
    
    if last_date is None:
        start_date = "2010-01-01"
        print(f"{stock_id} 資料庫空，從 2010-01-01 開始抓取資料")
    elif last_date >= today:
        print(f"{stock_id} 今日已更新過")
        return False
    else:
        start_date = last_date + timedelta(days=1)
        print(f"{stock_id} 從 {start_date} 開始更新資料")
    
    df = fetch_data(fetch_data_dict[mode], stock_id, start_date)
    
    if df.empty:
        return True

    # 若清理後沒資料就結束
    if df.empty:
        print(f"{stock_id} 清理後沒有可更新的資料")
        return True
    
    # 建立 upsert
    chunk_size = 5000

    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start+chunk_size]

        upsert_stmt = insert(table).values(chunk.to_dict(orient='records'))
        update_cols = {
            col.name: upsert_stmt.inserted[col.name]
            for col in table.columns if col.name != 'id'
        }
        upsert_stmt = upsert_stmt.on_duplicate_key_update(**update_cols)

        with engine.begin() as conn:
            conn.execute(upsert_stmt)

    print(f"{stock_id} 更新完成，新增/更新 {len(df)} 筆資料")
    return True


if __name__ == '__main__':
    # 建立 API 物件
    api = FinMindApi()

    # 登入（使用 token）
    token = config["api"]["token"]
    api.login_by_token(token)
    
    # 資料庫設定 
    DB_USER = config["mysql"]["user"]
    DB_PASS = config["mysql"]["pass"]
    DB_HOST = config["mysql"]["host"]
    DB_PORT = config["mysql"]["port"]
    DB_NAME = config["mysql"]["name"]

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        echo=False
    )
    
    # 建表（若已存在則跳過）
    create_all_tables()
    
    # 更新股票代碼
    if update_stock_codes(stock_codes_table):
        time.sleep(6)
    
    # 查詢所有股票代號
    query = "SELECT stock_id FROM stock_codes"
    stock_codes = pd.read_sql(query, engine)
    
    for stock in stock_codes["stock_id"]:
        if update_daily_data(stock, "stock_prices", stock_prices_table):
            time.sleep(6)
        if update_daily_data(stock, "margin_short", margin_short_table):
            time.sleep(6)
        if update_daily_data(stock, "institutional_trades", institutional_trades_table):
            time.sleep(6)
        if update_daily_data(stock,"month_revenue", month_revenue_table):
            time.sleep(6)
        if update_daily_data(stock,"per", per_table):
            time.sleep(6)
        if update_daily_data(stock,"financial_statements", financial_statements_table):
            time.sleep(6)
        if update_daily_data(stock,"balance_sheet", balance_sheet_table):
            time.sleep(6)
        if update_daily_data(stock,"cash_flow",cash_flow_table):
            time.sleep(6)
            
    if update_daily_data("TAIEX", "TAIEX", TW_index_table):
        time.sleep(6)
    update_daily_data("TPEx", "TPEx", TW_index_table)
