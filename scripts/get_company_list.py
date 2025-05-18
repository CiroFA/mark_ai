import requests
import csv
import os

# 📁 Percorso assoluto alla cartella /data/documents_raw
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "documents_raw")
os.makedirs(DATA_DIR, exist_ok=True)

save_path = os.path.join(DATA_DIR, "company_list.csv")

# 🔐 Headers richiesti dalla SEC
headers = {'User-Agent': 'Gerardo DArco gerardo@email.com'}
url = "https://www.sec.gov/files/company_tickers.json"

# 🔽 Scarica i dati
response = requests.get(url, headers=headers)
data = response.json()

# 💾 Scrive su CSV
with open(save_path, "w", newline='', encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["ticker", "cik"])
    for entry in data.values():
        ticker = entry['ticker']
        cik = str(entry['cik_str']).zfill(10)
        writer.writerow([ticker, cik])

print(f"✅ Salvato: {os.path.abspath(save_path)}")
