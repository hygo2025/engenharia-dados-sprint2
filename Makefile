# Nome do ambiente virtual
VENV := .venv

# Nome do arquivo de requisitos
REQUIREMENTS := requirements.txt

.PHONY: all clean create_venv install_deps install run custom_clean

# Comando principal
all: create_venv install_deps

# Criação do ambiente virtual
create_venv:
	python3 -m venv $(VENV)

# Instalação das dependências usando venv
install_deps: create_venv
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r $(REQUIREMENTS)

# Limpeza do projeto (venv e caches)
clean: custom_clean
	rm -rf $(VENV)
	find . -type d -name '__pycache__' -exec rm -r {} +
	find . -type f -name '*.pyc' -delete

# Instalação das dependências sem venv
install:
	pip install -r $(REQUIREMENTS)

# Executar o script principal
run:
	python scripts/main.py

# Limpeza customizada de arquivos CSV em data/
custom_clean:
	rm -rf data/**/*.csv
