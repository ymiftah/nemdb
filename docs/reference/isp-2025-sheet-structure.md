# ISP 2025 Input Assumptions — Sheet Structure Reference

**File:** `src/nemdb/artefacts/ISP_2025.xlsm`
**Total sheets:** 83
**Publisher:** AEMO (Australian Energy Market Operator)
**Context:** Modelling assumptions for the 2025 Integrated System Plan (ISP)

---

## Common Layout Conventions

Almost every data sheet shares this preamble structure:

| Row | Content |
|-----|---------|
| 0 | "Go to Assumptions Summary" (navigation link) |
| 1 | Sheet title |
| 2–N | Notes and caveats (free text) |
| N+1 | (sometimes) Scenario selector or consultant mapping sub-table |
| N+2 | Section label (e.g. "Existing generators") — only on multi-section sheets |
| N+3 | Column header row |
| N+4+ | Data rows |

**Year columns:** Time-series sheets use financial year labels as column headers spanning **2025-26 to 2054-55** (30 years), read as strings like `"2025-26"`.

**Scenarios:** Three modelling scenarios appear throughout:

- `Slower Growth`
- `Step Change`
- `Accelerated Transition`

---

## Sheet Inventory by Pattern Type

### Pattern A — Text / Reference Only

No tabular data to extract.

| Sheet | Rows×Cols | Description |
|-------|-----------|-------------|
| Disclaimer | 37×1 | Legal disclaimer text |
| Demand and Energy Forecasts | 7×4 | Pointer to external AEMO Forecasting Portal |

---

### Pattern B — Version / Changelog

Single simple 3-column table: Version Number, Date, Description.

| Sheet | Rows×Cols | Header row | Key columns |
|-------|-----------|-----------|-------------|
| Change Log | 606×4 | row 1 | `Version Number`, `Date`, `Description` |
| Assumptions Summary | 141×4 | row 5 | `Version Number`, `Date`, `Description` — plus links to other sheets |

---

### Pattern C — Scenario Comparison Table

Narrow table with one row per parameter, columns = scenario names.

| Sheet | Rows×Cols | Header row | Columns |
|-------|-----------|-----------|---------|
| Scenarios | 202×4 | row 4 | `Parameter`, `Slower Growth`, `Step Change`, `Accelerated Transition` |

---

### Pattern D — Generator / Technology Property Table (static columns)

One row per generator or technology. No year columns. Key identifier column in col 0.

| Sheet | Rows×Cols | Data starts row | Key identifier | Description |
|-------|-----------|-----------------|----------------|-------------|
| Existing Gen Data Summary | 655×45 | ~10 | IASR ID | Summary of existing, committed & anticipated generators — see below for column list |
| New Entrant Data Summary | 528×53 | ~10 | IASR ID | Summary of new entrant technologies |
| New Electrolyser Data Summary | 158×42 | ~4 | IASR ID | New electrolysers |
| Storage properties | 43×9 | ~6 | Technology | Charge/discharge efficiency, round-trip efficiency |
| Maintenance | 32×8 | ~5 | Unit | Planned maintenance rates per season |
| Generator Reliability Settings | 90×12 | ~8 | IASR ID | Multiple sub-tables: unplanned long/short outages |
| Retirement | 656×9 | ~8 | IASR ID | Expected closure years by scenario |
| Heat rates | 657×8 | ~7 | IASR ID | `send-out heat rate (GJ/MWh)` |
| Auxiliary | 653×7 | ~6 | IASR ID | Auxiliary load factor |
| Emissions intensity | 660×7 | ~6 | IASR ID | Emissions intensity (tCO2-e/MWh) |
| Fixed OPEX | 654×8 | ~6 | IASR ID | Fixed operating cost ($/kW/year) |
| Variable OPEX | 654×7 | ~6 | IASR ID | Variable operating cost ($/MWh) |
| Affine Heat rates | 190×10 | ~7 | IASR ID | Piecewise heat rate coefficients |
| Max Ramp Rates | 189×9 | ~7 | IASR ID | Ramp rates (MW/min) by direction and condition |
| Coal Min Stable Level | 63×6 | ~7 | IASR ID | Minimum stable generation level |
| GPG Min Stable Level | 148×7 | ~7 | IASR ID | Minimum stable level for gas peakers |
| First-of-a-kind premium | 11×3 | ~6 | Technology | FOAK cost premium (%) |
| Lead time and project life | 35×7 | ~6 | Technology | Lead time (years), economic life, technical life |
| Financial parameters | 90×6 | ~8 | Parameter | WACC and other financial settings by scenario |
| Gas System Properties | 191×7 | ~6 | Gas node | Gas system node properties |
| Reserves | 14×2 | ~5 | Reserve type | Reserve margins |
| Transmission Reliability | 13×4 | ~5 | Flow path | Reliability limits |

#### Existing Gen Data Summary — selected columns

`IASR ID`, `Power Station`, `Technology Type`, `Fuel Type`, `Region`, `Sub-region`, `REZ Location`, `REZ ID`, `Status`, `REZ`, `Max capacity (MW)`, `Seasonal rating summer peak (MW)`, `Heat rate (GJ/MWh)`, `Fuel cost ($/GJ)`, `MLF`, `Auxiliary (%)`, `Fixed OPEX ($/kW/year)`, `Variable OPEX ($/MWh)`, `Emissions intensity (tCO2-e/MWh)`, `Expected retirement year`, `Connection cost ($)`, `SRMC ($/MWh)`

---

### Pattern E — Time-Series Wide Table (generator/technology × year)

One row per entity, year columns from 2025-26 to 2054-55. Header row immediately precedes data.

| Sheet | Rows×Cols | Data start row | Row key | Sub-sections |
|-------|-----------|---------------|---------|--------------|
| Fuel Price Summary | 1178×36 | row 11 (existing), row 660 (new entrant) | Generator | 2 sub-tables: `Existing generators` (rows 11–658) + `New entrant generators` (rows 660–1177) |
| Coal and Biomass price | 61×32 | ~6 | Coal basin/scenario | Single table |
| Gas, Liquid fuel, H2 price | 386×33 | varies | Fuel node | Multiple sub-tables by fuel type |
| Build costs | 76×34 | ~8 | Technology | Single table with technology + year cols |
| Connection cost forecasts | 385×35 | varies | REZ ID | Time-series of connection costs |
| Flow path cost forecasts | 335×34 | varies | Flow path | Time-series of augmentation costs |
| REZ cost forecasts | 316×35 | varies | REZ ID | Time-series of REZ augmentation costs |
| Distribution cost forecasts | 84×35 | varies | Sub-region | Time-series |
| Seasonal ratings | 687×34 | ~8 | IASR ID | 4 season columns × ... — see note below |
| Marginal Loss Factors | 663×18 | varies | IASR ID | Multiple sub-tables by generator type/scenario |
| Locational Cost Factors | 225×23 | varies | Region/Technology | Cost scaling factors |
| GPG emissions reduction - BioM | 12×31 | ~6 | Technology | Single small table with year cols |
| Power System Security | 94×30 | ~3 | DUID | Multiple sub-tables: coal retirement costs + security constraint costs |
| H2 as fuel for GPG Limit | 21×32 | ~5 | Sub-region | Year columns |
| Build Cost - Hydrogen pipeline | 157×35 | varies | Route | Time-series pipeline costs |

> **Seasonal ratings note:** Columns are `Summer Peak`, `Summer Typical`, `Shoulder`, `Winter` — not year-labelled; instead years are represented by `Year` selector. Raw data has these 4 season columns, per generator row.

---

### Pattern F — Time-Series Wide, Multiple Scenarios (subregion/state × year per scenario)

The most common pattern for demand-side and DER forecasts. Each sheet has **multiple tables** stacked vertically, one per scenario (and sometimes per metric). The scenario label appears as a row above the header row for each sub-table.

**General layout:**

```text
row 0:  "Go to Assumptions Summary"
row 1:  Sheet title
row 2-N: Notes
row N+1: Consultant mapping table (optional)
row N+K:      [Section label, e.g. "Gross State Product ($ millions)"]
row N+K+1:    [Scenario label, e.g. "Slower Growth"]
row N+K+2:    [blank]
row N+K+3:    [Column header: key-col | 2025-26 | 2026-27 | ... | 2054-55]
row N+K+4+:   Data rows (one per subregion/state)
...repeated for each scenario and metric...
```

| Sheet | Rows×Cols | Metrics | Scenarios | Row key |
|-------|-----------|---------|-----------|---------|
| Economic Growth Forecasts | 66×32 | GSP, HDI | 3 | State |
| End use fuel consumption data | 39×31 | 1 (fuel cons.) | 3 | Fuel sector |
| Appliance Uptake Forecasts | 39×32 | 1 | 3 | Appliance type |
| Elec. Retail Price Indices | 13×32 | 1 | 3 | Sub-region |
| Connections Forecasts | 40×33 | 1 | 3 | Sub-region |
| Energy Efficiency | 77×32 | multiple | 3 | Sub-region |
| Rooftop PV | 116×33 | Capacity (MW), Energy (GWh) | 3 | Sub-region |
| PVNSG | 116×33 | Capacity, Energy | 3 | Sub-region |
| ONSG | 74×33 | Capacity, Energy | 3 | Sub-region |
| Battery & Plug-in EVs | 115×33 | multiple | 3 | Sub-region |
| Fuel cell EVs | 62×33 | multiple | 3 | Sub-region |
| EV V2G | 115×33 | multiple | 3 | Sub-region |
| DSP | 164×34 | multiple | 3 | Sub-region |
| Data Centre Forecasts | 32×31 | 1 | 3 | Sub-region |
| Electrification | 37×31 | 1 | 3 | Sub-region |
| Embedded energy storages | 116×33 | Capacity, Energy | 3 | Sub-region |
| Aggregated energy storages | 115×33 | Capacity, Energy | 3 | Sub-region |
| Hydrogen demand - Domestic | 53×33 | 1 (Mt/year) | 3 | Sub-region |
| Hydrogen demand-Export&Commod | 156×33 | multiple | 3 | Sub-region |
| Water for Hydrogen | 52×33 | 1 | 3 | Sub-region |
| Desalination demand for H2 | 52×33 | 1 | 3 | Sub-region |
| Other hydrogen assumptions | 35×31 | multiple | 3 | Sub-region |
| Coal and Biomass price | 61×32 | 1 | varies | Coal basin |
| Hydrogen monthly profiles | 44×32 | 1 | 3 | Month |

---

### Pattern G — Energy Policy Targets / Carbon Budgets (multi-jurisdiction)

Similar to Pattern F but split by jurisdiction rather than scenario.

| Sheet | Rows×Cols | Sections | Key |
|-------|-----------|---------|-----|
| Energy Policy Targets | 310×6 | Australia, NSW, QLD, SA, TAS, VIC | Target type × year |
| Carbon Budgets | 31×6 | NEM-wide + state budgets | State |

**Energy Policy Targets structure:**

- Rows 12–112: National (Australia) targets
- Rows 113–194: NSW
- Rows 195–202: QLD
- Rows 203–219: SA
- Rows 220–250: TAS
- Rows 251+: VIC

---

### Pattern H — Network Topology (simple reference tables)

| Sheet | Rows×Cols | Header row | Key columns |
|-------|-----------|-----------|-------------|
| Network representation | 82×4 | ~4 | `Flow path definition`, `Definition` |
| Renewable energy zones | 61×7 | ~5 | `REZ ID`, `REZ Name`, `State`, `NEM Region`, `Sub-region`, `Zone type` |
| Network capability | 151×21 | ~5 | `Flow Paths` + multiple capability columns |
| Network losses | 88×9 | ~6 | `Flow Path`, `Interconnector Name`, loss coefficients |
| Distribution network | 52×7 | ~6 | `Sub-region`, `NEM Region`, network characteristics |
| Connection cost | 71×17 | ~6 | `REZ ID`, `REZ names` + technology cost columns |
| Flow path augmentation options | 134×16 | ~9 | `Option ID`, `Flow path`, `Transfer level increase`, cost estimates |
| REZ augmentations options | 133×14 | ~7 | `REZ ID`, `REZ Name`, `Technology`, capacity |
| Maximum capacity | 667×14 | ~8 | `IASR ID`, `Power Station`, `Technology`, `Max capacity (MW)` |
| Hydro Scheme Inflows | 162×19 | ~8 | Reference year + monthly columns |
| Capacity Factors | 210×21 | ~6 | `REZ ID / Subregion`, `REZ Name` + technology capacity factor columns |
| Build limits - REZs | 257×14 | ~5 | `REZ ID`, `REZ Name` + technology build limit columns |
| Build limits - PHES | 27×22 | ~5 | `PHES site` + multiple duration/location columns |
| Hydrogen consumption locations | 57×7 | ~6 | `Sub-region`, `Region`, location attributes |

---

### Pattern I — Regional Build Costs Summary (view/calculated sheet)

**Regional Build Costs Summary** (74×48) is a dynamic view sheet that applies locational cost factors to baseline technology costs. It has scenario and technology dropdown selectors in the preamble, so it is not a primary source — raw data should be extracted from `Build costs` + `Locational Cost Factors` instead.

---

### Pattern J — Summary Mapping (master cross-reference)

**Summary Mapping** (1384×31) is the most comprehensive sheet. Starting at row 3, it contains one row per generator/technology across all categories (existing, committed, anticipated, new entrant) and provides cross-references to values in all other sheets.

Key columns: `RowID`, `IASR ID / DLT names`, `Power Station`, `Technology Type`, `Region`, `Sub-region`, `REZ Location`, `REZ ID`, `Status`, `MLF`, `Auxiliary load (%)`, `Connection cost`, `Build limit`, `Total lead time`

---

## Sheet Groupings by Thematic Domain

| Domain | Sheets |
|--------|--------|
| **Administrative** | Disclaimer, Change Log, Assumptions Summary, Scenarios |
| **Generator Registry** | Summary Mapping, Existing Gen Data Summary, New Entrant Data Summary, New Electrolyser Data Summary, Retirement |
| **Generator Technical** | Maximum capacity, Seasonal ratings, Heat rates, Affine Heat rates, Auxiliary, Emissions intensity, Storage properties, Max Ramp Rates, Coal Min Stable Level, GPG Min Stable Level, Maintenance, Generator Reliability Settings, Hydro Scheme Inflows, Capacity Factors |
| **Generator Costs** | Build costs, Regional Build Costs Summary, Fixed OPEX, Variable OPEX, Locational Cost Factors, First-of-a-kind premium, Lead time and project life, Financial parameters |
| **Fuel Prices** | Fuel Price Summary, Coal and Biomass price, Gas Liquid fuel H2 price, Gas System Properties, GPG emissions reduction - BioM |
| **Network** | Network representation, Renewable energy zones, Network capability, Network losses, Transmission Reliability, Distribution network, Marginal Loss Factors, Connection cost, Connection cost forecasts |
| **Network Development** | Flow path augmentation options, Flow path cost forecasts, REZ augmentations options, REZ cost forecasts, Distribution cost forecasts, Build limits - REZs, Build limits - PHES |
| **Demand & DER Forecasts** | Demand and Energy Forecasts, Economic Growth Forecasts, End use fuel consumption data, Appliance Uptake Forecasts, Elec. Retail Price Indices, Connections Forecasts, Energy Efficiency, Rooftop PV, PVNSG, ONSG, Battery & Plug-in EVs, Fuel cell EVs, EV V2G, DSP, Data Centre Forecasts, Electrification, Embedded energy storages, Aggregated energy storages |
| **Energy Policy** | Energy Policy Targets, Carbon Budgets, Power System Security, Reserves |
| **Hydrogen** | Hydrogen demand - Domestic, Hydrogen monthly profiles, Hydrogen demand-Export&Commod, Hydrogen consumption locations, Water for Hydrogen, Desalination demand for H2, H2 as fuel for GPG Limit, Build Cost - Hydrogen pipeline, Other hydrogen assumptions |
