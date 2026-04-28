# Project Info CSVs — Data Fusion Source

Place one CSV per city here. The pipeline will automatically left-join it to presale transactions on `建案名稱`.

## File naming

| City     | Filename          |
|----------|-------------------|
| 臺北市   | `Taipei.csv`      |
| 新北市   | `New_Taipei.csv`  |
| 桃園市   | `Taoyuan.csv`     |
| 臺中市   | `Taichung.csv`    |
| 臺南市   | `Tainan.csv`      |
| 高雄市   | `Kaohsiung.csv`   |

## Required columns

| Column   | Maps to          | Notes                                      |
|----------|------------------|--------------------------------------------|
| 建案名稱 | join key         | Must match MOI presale `建案名稱` exactly  |
| 起造人   | Developer        | First 4 chars used; individuals → `個體/合作開發` |
| 層棟戶數 | ProjectScale     | Unit count extracted (e.g. `120戶` → `120`) |
| 使用分區 | ZoningTag        | `住*` → 住 (green), `商*` → 商 (yellow), others → first 4 chars (gray) |

## Example `New_Taipei.csv`

```csv
建案名稱,起造人,層棟戶數,使用分區
合康世紀,合康建設股份有限公司,1棟/68戶,住宅區
富霖富鄰,富霖建設開發有限公司,2棟/96戶,住宅區
```

## Notes
- Encoding: UTF-8 with BOM (utf-8-sig), UTF-8, Big5, or CP950 — all accepted.
- Duplicate `建案名稱` rows: only the **first** occurrence is used.
- Missing file: pipeline skips enrichment gracefully — `ProjectScale` and `ZoningTag` stay blank.
- After adding/updating a file, re-run `python main.py` to regenerate parquets.
