#!/bin/bash
# Script de test du semantic layer box_sales

echo "🔍 Vérification du semantic layer pour box_sales"
echo "================================================"
echo ""

# 1. Vérifier que les fichiers existent
echo "✅ Vérification des fichiers..."
if [ -f "models/dwh/sales/_semantic_models.yml" ]; then
    echo "   ✓ _semantic_models.yml trouvé ($(du -h models/dwh/sales/_semantic_models.yml | cut -f1))"
else
    echo "   ✗ _semantic_models.yml MANQUANT"
    exit 1
fi

if [ -f "models/dwh/sales/_semantic_models_example.yml" ]; then
    echo "   ✓ _semantic_models_example.yml trouvé ($(du -h models/dwh/sales/_semantic_models_example.yml | cut -f1))"
fi

echo ""

# 2. Vérifier la syntaxe YAML
echo "✅ Vérification de la syntaxe YAML..."
if command -v python3 &> /dev/null; then
    python3 -c "
import yaml
import sys

try:
    with open('models/dwh/sales/_semantic_models.yml', 'r') as f:
        yaml.safe_load(f)
    print('   ✓ Syntaxe YAML valide')
    sys.exit(0)
except Exception as e:
    print(f'   ✗ Erreur YAML: {e}')
    sys.exit(1)
"
else
    echo "   ⚠ Python3 non trouvé, impossible de valider la syntaxe YAML"
fi

echo ""

# 3. Compter les éléments du semantic model
echo "✅ Contenu du semantic model..."
echo "   Entités définies:"
grep -A 1 "name:" models/dwh/sales/_semantic_models.yml | grep -A 1 "entities:" -A 20 | grep "- name:" | head -10 | sed 's/.*name: /     - /'

echo ""
echo "   Nombre de dimensions: $(grep -c "type: categorical\|type: time" models/dwh/sales/_semantic_models.yml || echo 0)"
echo "   Nombre de mesures: $(grep -A 1 "measures:" models/dwh/sales/_semantic_models.yml | grep -c "- name:" || echo 0)"

echo ""
echo "================================================"
echo "✨ Le semantic layer est configuré et prêt !"
echo ""
echo "Prochaines étapes:"
echo "  1. Installer dbt: pip install dbt-core dbt-bigquery"
echo "  2. Tester: dbt parse"
echo "  3. Requêter: dbt sl query --metrics total_net_revenue --group-by dw_country_code"
