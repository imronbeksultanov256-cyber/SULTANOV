import os, json, re, hashlib
from pathlib import Path
from typing import List, Dict

from dotenv import load_dotenv
from pypdf import PdfReader
import docx

load_dotenv()

KNOWLEDGE_DIR = Path("knowledge")
INDEX_PATH = Path("kb_index.json")

def read_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")

def read_pdf(path: Path) -> str:
    reader = PdfReader(str(path))
    parts = []
    for page in reader.pages:
        parts.append(page.extract_text() or "")
    return "\n".join(parts)

def read_docx(path: Path) -> str:
    d = docx.Document(str(path))
    return "\n".join(p.text for p in d.paragraphs)

def clean_text(t: str) -> str:
    t = t.replace("\u00a0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()

def chunk_text(text: str, chunk_chars: int = 1200, overlap: int = 150) -> List[str]:
    text = clean_text(text)
    if not text:
        return []
    chunks = []
    i = 0
    while i < len(text):
        chunk = text[i:i+chunk_chars]
        chunks.append(chunk)
        i += (chunk_chars - overlap)
    return chunks

def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()[:16]

def load_file(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".txt":
        return read_txt(path)
    if ext == ".pdf":
        return read_pdf(path)
    if ext == ".docx":
        return read_docx(path)
    return ""

def main():
    if not KNOWLEDGE_DIR.exists():
        print("Создай папку knowledge и положи туда файлы.")
        return

    items: List[Dict] = []
    for p in sorted(KNOWLEDGE_DIR.rglob("*")):
        if p.is_dir():
            continue
        if p.suffix.lower() not in [".txt", ".pdf", ".docx"]:
            continue

        raw = load_file(p)
        raw = clean_text(raw)
        if not raw:
            print(f"⚠️ Не удалось извлечь текст: {p.name}")
            continue

        chunks = chunk_text(raw)
        fh = file_hash(p)
        for idx, ch in enumerate(chunks):
            items.append({
                "id": f"{p.name}:{fh}:{idx}",
                "source": p.name,
                "chunk_index": idx,
                "text": ch
            })

        print(f"✅ {p.name}: {len(chunks)} фрагм.")

    INDEX_PATH.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nГотово! Индекс сохранён в {INDEX_PATH}")

if __name__ == "__main__":
    main()