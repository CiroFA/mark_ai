import csv
import os
import yfinance as yf

input_path = os.path.join(os.path.dirname(__file__), '../data/documents_raw/company_list.csv')
output_path = os.path.join(os.path.dirname(__file__), '../data/documents_raw/company_list_with_names.csv')

with open(input_path, newline='', encoding='utf-8') as infile, \
     open(output_path, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ['name'] if 'name' not in reader.fieldnames else reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        ticker = row['ticker']
        try:
            info = yf.Ticker(ticker).info
            name = info.get('shortName', 'N/A')
        except Exception as e:
            print(f"Errore su {ticker}: {e}")
            name = 'N/A'

        row['name'] = name
        writer.writerow(row)

print(f"✅ File aggiornato con nomi salvato in: {output_path}")