import os
import textwrap

# 📁 Imposta i percorsi in base alla struttura del progetto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

INPUT_FOLDER = os.path.join(ROOT_DIR, "data", "text_clean")
OUTPUT_FOLDER = os.path.join(ROOT_DIR, "data", "chunks")
MAX_CHARS = 3000

# 📂 Crea la cartella di output se non esiste
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# 🔧 Divide paragrafi troppo lunghi
def safe_split_paragraph(para, max_chars):
    return textwrap.wrap(para, width=max_chars, break_long_words=False, replace_whitespace=False)

# ✂️ Funzione per creare blocchi da max 3000 caratteri mantenendo coerenza semantica
def chunk_by_paragraphs(text, max_chars=MAX_CHARS):
    paragraphs = text.split("\n\n")
    chunks = []
    current_chunk = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if len(para) > max_chars:
            for sub_para in safe_split_paragraph(para, max_chars):
                if current_chunk:
                    chunks.append(current_chunk.strip())
                    current_chunk = ""
                chunks.append(sub_para.strip())
        elif len(current_chunk) + len(para) + 2 > max_chars:
            chunks.append(current_chunk.strip())
            current_chunk = para
        else:
            current_chunk += "\n\n" + para

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks

# 🔁 Applica chunking a tutti i file .txt presenti in text_clean/
def process_all_files():
    for file in os.listdir(INPUT_FOLDER):
        if not file.endswith(".txt"):
            continue

        input_path = os.path.join(INPUT_FOLDER, file)
        base_name = file.replace(".txt", "")
        chunk_already_present = any(f.startswith(base_name) for f in os.listdir(OUTPUT_FOLDER))
        if chunk_already_present:
            continue

        with open(input_path, "r", encoding="utf-8") as f:
            text = f.read()

        chunks = chunk_by_paragraphs(text)

        for i, chunk in enumerate(chunks):
            chunk_filename = f"{base_name}_chunk{i+1}.txt"
            output_path = os.path.join(OUTPUT_FOLDER, chunk_filename)
            with open(output_path, "w", encoding="utf-8") as out:
                out.write(chunk)

        print(f"[CHUNKED] {file} → {len(chunks)} blocchi")

# ▶️ Entry point
if __name__ == "__main__":
    process_all_files()
