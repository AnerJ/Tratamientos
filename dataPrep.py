import csv
import re

# Ruta del archivo original
input_file = "02-the-two-towers.txt"
output_file = "two_towers_chapters_unique.csv"

# Variables para guardar resultados
chapters = []
chapter_id = None
chapter_title = ""
chapter_lines = []

# Patrón para detectar inicio de capítulo
chapter_pattern = re.compile(r'^Chapter\s+\d+', re.IGNORECASE)

with open(input_file, "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    line = line.strip()
    if chapter_pattern.match(line):
        # Guardar capítulo anterior
        if chapter_id is not None:
            chapters.append({
                "chapter_id": chapter_id,
                "chapter_title": chapter_title,
                "text": " ".join(chapter_lines)
            })

        # Iniciar nuevo capítulo
        chapter_id = line.replace("Chapter", "").strip()
        chapter_title = lines[i + 1].strip() if i + 1 < len(lines) else ""
        chapter_lines = []
    elif chapter_id is not None:
        chapter_lines.append(line)

# Agregar el último capítulo
if chapter_id is not None:
    chapters.append({
        "chapter_id": chapter_id,
        "chapter_title": chapter_title,
        "text": " ".join(chapter_lines)
    })

# Guardar en CSV
with open(output_file, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["chapter_id", "chapter_title", "text"])
    writer.writeheader()
    for chapter in chapters:
        writer.writerow(chapter)
