#!/bin/bash
# Navegar al directorio donde esta este script
cd "$(dirname "$0")"
# Ejecutar recorder.py usando el Python del entorno virtual (que tiene las librerias)
../venv/bin/python3 recorder.py
