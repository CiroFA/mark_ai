import os
from bs4 import BeautifulSoup

# 📁 Percorsi delle cartelle
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

RAW_FOLDER = os.path.join(ROOT_DIR, "data", "documents_raw")
CLEAN_FOLDER = os.path.join(ROOT_DIR, "data", "text_clean")

os.makedirs(CLEAN_FOLDER, exist_ok=True)

def extract_text_from_filing(html_content):
    soup = BeautifulSoup(html_content, "lxml")

    # Rimuove tag non rilevanti
    for tag in soup(["script", "style", "table", "img", "ix:nonNumeric", "ix:nonFraction"]):
        tag.decompose()
    for tag in soup.find_all():
        if tag.name and tag.prefix == "ix":
            tag.decompose()

    # Estrae il testo
    text = soup.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines()]
    non_empty = [line for line in lines if line and not line.startswith("SEC.gov")]

    # Inizia da FORM 10-K, 10-Q, ecc.
    start_keywords = ["FORM 10-K", "FORM 10-Q", "UNITED STATES SECURITIES AND EXCHANGE COMMISSION"]
    for i, line in enumerate(non_empty):
        if any(keyword in line for keyword in start_keywords):
            return "\n".join(non_empty[i:])

    # fallback se nessuna keyword trovata
    return "\n".join(non_empty)

def main():
    for filename in os.listdir(RAW_FOLDER):
        if not filename.endswith(".html"):
            continue

        input_path = os.path.join(RAW_FOLDER, filename)
        output_path = os.path.join(CLEAN_FOLDER, filename.replace(".html", ".txt"))

        if os.path.exists(output_path):
            print(f"[SKIP] {filename} già estratto")
            continue

        with open(input_path, "r", encoding="utf-8") as f:
            html = f.read()

        cleaned_text = extract_text_from_filing(html)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned_text)

        print(f"[EXTRACTED] {filename} ➜ {output_path}")

if __name__ == "__main__":
    main()
