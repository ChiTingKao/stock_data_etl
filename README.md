本專案利用 FinMind API 與 MySQL 建立完整的台股資料庫 ETL（Extract, Transform, Load）流程，
可自動抓取台灣上市櫃股票之：

- 歷史股價
- 法人買賣超
- 融資融券
- 月營收
- 本益比、股價淨值比等財務比率
- 財報 / 資產負債表 / 現金流量表
- 加權報酬指數（TAIEX）及上櫃報酬指數（TPEx）

並支援自動增量更新：每日僅抓取資料庫不存在的日期。

---

每次 API 呼叫後自動 `sleep(6)` 秒，以符合 FinMind API 限制。

---

config.ini 格式

以下為必要設定：

[api]
token = your_finmind_token_here

[mysql]
user = root
pass = your_password
host = 127.0.0.1
port = 3306
name = stockdb

---

資料表清單

| Table Name           | 描述                |
| -------------------- | ----------------- |
| stock_codes          | 股票代碼與基本資訊         |
| stock_prices         | 歷史股價（日）           |
| TW_index             | 加權/上櫃報酬指數         |
| margin_short         | 融資融券資料            |
| institutional_trades | 法人買賣超             |
| month_revenue        | 月營收               |
| per                  | PER / PBR / 股利殖利率 |
| financial_statements | 綜合損益表             |
| balance_sheet        | 資產負債表             |
| cash_flow            | 現金流量表             |

---
