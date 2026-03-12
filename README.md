# Pharma Analytics App (Excel, No DB)

This app analyzes pharma sales and activity data from uploaded Excel files without a database. Uploaded sheets are stored locally as flat files and merged into one analytics dataset.

## Features
- Excel upload (`.xlsx` / `.xls`)
- Local flat-file persistence for uploaded sheets
- User-selectable storage location: local folder or Amazon S3
- Upload history with file, sheet, row count, and import timestamp
- Duplicate import protection using file-content hash + sheet name
- Combined analytics across all imported files
- Filter by date range and business dimensions
- Time windows: month, quarter, half-year, year
- Analytics:
  - Units sold by `chemist` (by `product`)
  - Units sold by `stockist` (by `product`)
  - Units sold by `medical_rep` (by `product`, inferred)
  - Product sales by geographic region (`territory` / `cnf` / `stockist` / `chemist`) across time windows
- Export filtered data as CSV

## Local Storage Model
- Original uploaded Excel files are stored in `data/uploads/`
- Normalized imported sheets are stored as CSV files in `data/normalized/`
- Import history is stored in `data/upload_log.csv`
- The merged analytics dataset is stored in `data/master_data.csv`

When you reopen the app on the same machine, previously imported files remain available.

## Cloud Storage Option
The app can also store uploads in Amazon S3. In the UI, the user can choose the storage location if S3 is configured.

Set these environment variables locally, or add them as Streamlit secrets when deployed:

- `S3_BUCKET`
- `S3_PREFIX` (optional, default: `pharma-analytics`)
- `AWS_DEFAULT_REGION` (optional)
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` if needed

When `S3_BUCKET` is configured, the app shows an `Amazon S3` storage option. The upload log, normalized files, and merged dataset are stored in that S3 location instead of local disk.

## Required/Supported Columns
The app normalizes column names (case-insensitive, spaces allowed).

Recommended columns:
- `date`
- `territory`
- `cnf`
- `stockist`
- `chemist`
- `doctor`
- `medical_rep`
- `product`
- `units`

Optional geo columns for doctor-chemist proximity inference:
- `doctor_lat`, `doctor_lon`
- `chemist_lat`, `chemist_lon`

Aliases supported:
- `medical rep` -> `medical_rep`
- `transaction_date` / `sale_date` -> `date`
- `qty` / `quantity` -> `units`

## Inference Logic for Doctor Prescription Attribution
- If a row already has `doctor`, units are directly attributed to that doctor.
- For rows with `chemist + product + units` but missing doctor:
  - candidate doctors are selected from the same `territory`
  - if lat/lon is present, candidate doctors are filtered by proximity threshold (km)
  - units are split equally among candidates
- doctor attribution is mapped to `medical_rep` using the most common doctor-rep pairing in the dataset.

## Run
```powershell
pip install -r requirements.txt
streamlit run app.py
```

## Build Windows EXE
The packaged EXE starts the Streamlit server locally and opens the app in the user's browser.

```powershell
powershell -ExecutionPolicy Bypass -File .\build_exe.ps1
```

Output:
- `dist\PharmaAnalytics.exe`

Build inputs:
- [launcher.py](d:\00_ANNAAPP\launcher.py)
- [build_exe.ps1](d:\00_ANNAAPP\build_exe.ps1)

Notes:
- The EXE is Windows-only.
- The app still runs as a local web app, even when launched from the EXE.
- Local flat-file data will be created relative to the folder where the app runs.

## Import Workflow
- Upload an Excel file
- Select the sheet to import
- Click `Import selected sheet`
- The app saves the original file, saves a normalized flat file, updates the upload log, and refreshes analytics across all imported sheets

## Notes
- No data is stored in a database.
- Keep `units` numeric and `date` parseable for best results.
