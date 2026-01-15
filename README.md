# big-data-real-estate
## Data management

Due to size constraints, raw and processed datasets (DVF, INSEE, parquet files) are not stored in this GitHub repository.

The project follows a data lake structure locally:

- `data/raw/` : raw source files (DVF, INSEE)
- `data/formatted/` : cleaned and standardized datasets
- `data/usage/` : datasets ready for analysis

All data-related folders are excluded via `.gitignore`.

## Project structure
big-data-real-estate/
│
├── data/              # local data lake (ignored by git)
│   ├── raw/
│   ├── formatted/
│   └── usage/
│
├── scripts/
│   ├── format_dvf.py
│   ├── format_insee.py
│   └── combine_dvf_insee.py
│
├── notebooks/         # exploration / analysis
├── dags/              # airflow DAGs (if any)
├── .gitignore
└── README.md

## Scripts

- `format_dvf.py`  
  Cleans and standardizes DVF raw files.

- `format_insee.py`  
  Cleans and prepares INSEE demographic data.

- `combine_dvf_insee.py`  
  Merges DVF real estate transactions with INSEE data at commune level.

## Data architecture
- Data lake (raw / formatted / usage)
- Data excluded from GitHub

## Analysis
- Spatial analysis by commune
- Temporal evolution of prices
- Correlation with demographic indicators
