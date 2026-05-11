# � RE AI Terminal · v3.2 | 台灣房產趨勢監測站

[English Version](#-english-version) | [中文版說明](#-中文版說明)

---

## 🏗️ 專案開發框架 (Project Framework)

本專案是一個整合 AI 估價模型與自動化數據流水線的房地產戰情室，旨在將破碎的台灣實價登錄原始數據 (LVR) 轉化為具備金融級洞察的視覺化終端。

### 核心技術棧 (Tech Stack)
* **Data Science**: 使用 `Python` & `Pandas` 進行大規模數據清洗、特徵工程及 `Parquet` 高效能數據存取。
* **AI Engine**: 自建 Oracle 估價演算法，執行多變數房價歸因分析、風險建模與趨勢預測。
* **DevOps**: 透過 `GitHub Actions` 實作自動化排程 (CI/CD)，每日定時抓取數據並自動部署。
* **Frontend**: 採用 `Tailwind CSS` 與 `Vanilla JS` 打造具備極高響應速度的金融感（Dark Mode）戰情介面。

---

## 🇹🇼 中文版說明

### 📊 Leaderboard 行政區排名邏輯
本系統的排行榜並非單純依據房價高低，而是透過以下邏輯進行動態加權：
* **資料期間**：鎖定 **滾動式近 6-12 個月** 的成交數據，確保排名反映的是「當下體質」而非歷史榮景。
* **權重邏輯**：綜合 **Resilience (抗跌性)**、**DOM (去化速度)** 以及 **Leverage (金融安全性)** 三大指標。
* **目標**：找出目前市場中「結構最穩健」而非「單價最高」的行政區。

### 🚀 核心指標深度解析 (Core Indicators)

#### 🌡️ Health Tiles (行政區健康燈號)
* **說明**：針對特定行政區的「綜合體質檢查」。
* **燈號意義**：🟢 **Healthy** (穩健) | 🟡 **Cautions** (關注) | 🔴 **Volatile** (波動)。

#### ⚖️ ORACLE 預測 (Oracle Prediction)
* **說明**：AI 模擬估價師，基於地點、屋齡、樓層建模。
* **估價公式**：

```math
FairValue = \beta_{1}(\text{Location}) + \beta_{2}(\text{Age}) + \beta_{3}(\text{Floor}) + \epsilon
```
#### 🔍 ORACLE模型邏輯拆解 (Logic Breakdown)
本模型的核心目標是剔除「隨機波動」，找出房產的**內在價值 (Intrinsic Value)**。公式各項代表意義如下：

* **$\beta_{1}(\text{Location})$ — 區域價值錨點**：
    根據行政區、鄰近捷運站距離及路段率進行權重校正。這是房價最基礎的組成部分。
* **$\beta_{2}(\text{Age})$ — 折舊係數**：
    自動計算屋齡對房價的負面邊際效應。隨著屋齡增加，建物價值會依據線性折舊邏輯進行修正。
* **$\beta_{3}(\text{Floor})$ — 垂直溢價**：
    考量高低樓層的景觀與環境差異。一般而言，高樓層具備更高的垂直溢價係數。
* **$\epsilon$ (Error Term) — 市場隨機擾動**：
    代表模型無法解釋的外部因素，如裝潢品質、特殊交易情況或極端市場情緒。

#### 💡 使用者導讀
透過此公式計算出的 **FairValue**，可以作為該物件的「理性能量尺」。當市場成交價（Actual Price）遠高於 FairValue 時，代表買方支付了額外的「情緒溢價」或「裝潢溢價」；反之，則是具備安全邊際的潛在標的。

#### 🏢 建案總覽 (Pre-sale Project Explorer) *(v3.2 新增)*
* **說明**：獨立標籤頁，依行政區列出所有預售建案一覽表。
* **欄位**：建案名稱、路段、建商（前4字）、戶數、分區、總樓層、單價(萬/坪)、主建比%、最新登錄日。
* **資料來源**：MOI 預售登錄 (`_b.csv`) 結合 `data/project_info/` 建商資料，每日自動更新。

#### ⏱️ DOM 短期% (Days on Market Short-term)
* **說明**：監測物件從掛牌到成交的「去化流動性」。數值越高代表買氣強勁；數值下降則代表買方進入觀望期。

#### 🔥 市場過熱訊號 (Market Heat Signals)
* **邏輯**：自動掃描溢價率、吸收率與成交動能。當 **4/5** 以上訊號啟動時，系統將點亮 **紅色🔥標記**。

#### 🛡️ RESILIENCE (價格韌性) & 📉 DRAWDOWN (歷史回撤)
* **說明**：衡量區域抗跌能力與目前價格距離歷史高點的修正幅度。

#### ⚙️ LEVERAGE% (槓桿風險燈號)
* **說明**：追蹤區域金融槓桿。🔴 **High** 代表金融擴張較快，反轉風險高。

---

### 🕒 自動化更新 (Automation)
* **週期**：系統於 **台灣時間每天早上 10:00 (UTC+8 / 02:00 UTC)** 定時執行，由 `update_data.yml` 統一排程。
* **流程**：ETag 比對（偵測 MOI 資料是否更新）→ 下載並清洗 LVR 資料 → 生成 `sample_debug.json` 驗證 → 自動部署至 GitHub Pages。
* **Fail-fast 機制**：若 `sample_debug.json` 驗證失敗（實際/預售樣本為空），CI 流程中止，避免部署損壞的靜態檔案。

---

### ⚖️ 授權協定與聲明 (License & Terms)

本專案採用 **[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)](https://creativecommons.org/licenses/by-nc-sa/4.0/deed.zh_TW)** 授權。

#### **當您使用本專案時，代表您同意以下約束：**
1.  **姓名標示 (Attribution)**：您必須給予原作者（eli20261124 / spoky119@gmail.com）適當標示。
2.  **非商業性 (Non-Commercial)**：**嚴禁將本專案之原始碼、分析結果、數據模型或網頁介面用於任何形式的盈利行為**。包括但不限於：收費軟體封裝、數據轉售、付費訂閱服務或作為商業房地產報告之核心。
3.  **相同方式分享 (ShareAlike)**：若您改作、轉換或依本素材建立新素材，您必須依本專案採用的相同授權條款來散佈您的貢獻。
4.  **免責聲明**：本專案提供之所有數據與預測僅供學術研究與參考之用，不構成專業投資建議。用戶依據本資料進行投資所造成之任何損益，開發者概不負責。

**📩 商業授權洽詢**：
若有商業合作需求、數據 API 對接、或欲取得豁免於「非商業性限制」的授權，請聯繫：**spoky119@gmail.com**。

---

## 🇺🇸 English Version

### 🚀 Key Performance Indicators (v3.2)
* **Health Tiles**: 🟢 Healthy | 🟡 Cautions | 🔴 Volatile.
* **Oracle Prediction**: AI-driven "Fair Value" modeling.
* **DOM Short-term %**: Measures sales velocity and market liquidity.
* **Resilience & Drawdown**: Evaluates price stability and correction from historical peaks.
* **Pre-sale Project Explorer** *(new in v3.2)*: Dedicated tab listing all pre-sale projects per district — developer, unit count, zoning, price/ping, main building ratio, and latest registration date.

## 🚀 What's New in v3.2

**RE AI Terminal** 迎來了 v3.2 版本的重大更新，專注於擴展數據覆蓋範圍、優化終端機效能，並提升特殊交易的可讀性。

### 📊 Data Scope & Performance
*   **City Expansion (城市擴張)**：正式將資料管線延伸至 **新竹市 (Hsinchu)** 與 **基隆市 (Keelung)**，支援更廣泛的北台灣房地產市場分析。
*   **3-Year Rolling Window (三年動態視窗)**：為確保高效運算，資料庫深度全面優化為「近三年 (Last 3 Years)」的滾動式數據。確保趨勢指標 (Trend Indicators) 更貼近當前市場脈動。

### 🧹 Data Sanitization & UI
*   **Smart Special Transactions (智慧特殊交易標籤)**：重構了特殊交易的判定邏輯。系統不再只顯示單一警告，而是主動解析備註欄位，自動打上 `[關係人]` (如親友交易) 或 `[含增建]` 等精準標籤。
*   **Hover Tooltips (懸浮備註)**：前端 UI 新增懸浮提示功能，游標移至特殊交易警告上即可查看完整的原始官方備註內容。
*   **Developer Name Extraction (建商萃取機制)**：新增針對預售屋「起造人」欄位的字串萃取邏輯 (擷取前四碼)，為後續的建案字典 (Project Dictionary) 與品牌建商溢價分析打下基礎。

---
### 🔧 Fix: Data Persistence via GitHub Actions (2026-05-11)
*   **Root Cause**: The `update_data.yml` workflow was running the Python pipeline but not committing the generated `data/processed/` files back to the repository, so GitHub Pages was never receiving updated data.
*   **Fix Applied**: Replaced the "Commit refreshed data" step with a dedicated **"Commit and Push"** step using `git config --local`, `git add data/processed/`, and `git commit -m "auto: periodic data update"`. This ensures processed data is persisted to the repo after every successful pipeline run.

*Last updated: 2026-05-11*

### ⚖️ Licensing & Terms
This project is licensed under **CC BY-NC-SA 4.0**.
* **Attribution**: Credit must be given to the original creator.
* **Non-Commercial**: Commercial usage, data reselling, or paid-service packaging is **STRICTLY PROHIBITED**.
* **ShareAlike**: Derivatives must be distributed under the same license.
* **Contact for Licensing**: `spoky119@gmail.com`
