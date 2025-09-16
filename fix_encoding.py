from pathlib import Path

# Ajuste o caminho se necessário
path = Path(r"C:\Users\lucas.santiago_vissi\Documents\Monitoramento\.streamlit\secrets.toml")

# Lê como Latin-1 (ou Windows-1252) e grava em UTF-8 "sem BOM"
conteudo = path.read_text(encoding="latin1")
path.write_text(conteudo, encoding="utf-8")

print("Convertido para UTF-8 com sucesso!")
