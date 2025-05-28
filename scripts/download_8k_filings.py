import os
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.sec.gov"
HEADERS = {'User-Agent': 'Your Name your.email@example.com'}  # metti il tuo nome/email reali qui

def get_cik_from_ticker(ticker):
    """
    Ottiene il CIK corrispondente a un ticker (es: AAPL → 0000320193)
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=HEADERS)
    data = response.json()
    for company in data.values():
        if company["ticker"].lower() == ticker.lower():
            return str(company["cik_str"]).zfill(10)
    return None

def download_8k_filings(ticker, save_dir="data/sec_8k", max_docs=5):
    """
    Scarica fino a max_docs filing 8-K per una determinata azienda (ticker)
    """
    cik = get_cik_from_ticker(ticker)
    if not cik:
        print(f"CIK non trovato per il ticker {ticker}")
        return

    os.makedirs(save_dir, exist_ok=True)
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    response = requests.get(submissions_url, headers=HEADERS)
    data = response.json()

    count = 0
    filings = data["filings"]["recent"]
    for i in range(len(filings["form"])):
        if filings["form"][i] != "8-K":
            continue

        accession = filings["accessionNumber"][i].replace("-", "")
        accession_full = filings["accessionNumber"][i]
        filing_date = filings["filingDate"][i]
        filing_url = f"{BASE_URL}/Archives/edgar/data/{int(cik)}/{accession}/{accession_full}-index.html"

        # scarica la pagina index
        index_page = requests.get(filing_url, headers=HEADERS).text
        soup = BeautifulSoup(index_page, "html.parser")

        # cerca tabella dei documenti
        table = soup.find("table", class_="tableFile")
        if table:
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 3 and ("8-K" in cols[0].text or "htm" in cols[1].text):
                    doc_href = cols[2].a["href"]
                    doc_url = BASE_URL + doc_href
                    doc_response = requests.get(doc_url, headers=HEADERS)
                    file_name = f"{ticker}_8-K_{filing_date}.html"
                    with open(os.path.join(save_dir, file_name), "w", encoding="utf-8") as f:
                        f.write(doc_response.text)
                    print(f"✔️ Salvato: {file_name}")
                    count += 1
                    break  # solo primo documento rilevante

        if count >= max_docs:
            break

if __name__ == "__main__":
    download_8k_filings("AAPL")
