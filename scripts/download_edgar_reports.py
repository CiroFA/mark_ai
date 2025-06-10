import csv
import os
import requests
import time

# 📁 Percorsi
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

CSV_PATH = os.path.join(ROOT_DIR, "data", "documents_raw", "company_list.csv")
SAVE_FOLDER = os.path.join(ROOT_DIR, "data", "documents_raw")
FORM_TYPES = ["10-K", "10-Q"]
MAX_COMPANIES = 120
HEADERS = {'User-Agent': 'Gerardo DArco gerardo@email.com'}

os.makedirs(SAVE_FOLDER, exist_ok=True)

def download_form(ticker, cik, form_type, already_files):
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(url, headers=HEADERS)
        data = r.json()

        recent = data["filings"]["recent"]
        forms = recent["form"]
        acc_nums = recent["accessionNumber"]
        docs = recent["primaryDocument"]

        for i in range(len(forms)):
            if forms[i] == form_type:
                acc_no = acc_nums[i].replace("-", "")
                filename = f"{ticker}_{form_type}_{acc_no}.html"
                path = os.path.join(SAVE_FOLDER, filename)

                if filename in already_files:
                    print(f"[SKIP] {filename} già presente")
                    return 0

                primary_doc = docs[i]
                filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no}/{primary_doc}"

                # 📥 Download del documento vero
                html = requests.get(filing_url, headers=HEADERS).text

                with open(path, "w", encoding="utf-8") as f:
                    f.write(html)

                print(f"[DOWNLOAD] {filename}")
                time.sleep(0.5)
                return 1

        return 0

    except Exception as e:
        print(f"[ERROR] {ticker} – {form_type} – {e}")
        return 0

def main():
    already_files = set(f for f in os.listdir(SAVE_FOLDER) if f.endswith(".html"))

    with open(CSV_PATH, "r") as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            if i >= MAX_COMPANIES:
                break

            ticker = row["ticker"]
            cik = row["cik"]

            for form_type in FORM_TYPES:
                prefix = f"{ticker}_{form_type}_"
                already_for_type = any(f.startswith(prefix) for f in already_files)

                if not already_for_type:
                    download_form(ticker, cik, form_type, already_files)
                else:
                    print(f"[SKIP] {ticker} – {form_type} già presente")

if __name__ == "__main__":
    main()
