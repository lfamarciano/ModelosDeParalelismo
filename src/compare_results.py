import json
import sys
import math

def compare_values(v1, v2, path):
    """Compara dois valores, com tolerância para floats."""
    if isinstance(v1, float) and isinstance(v2, float):
        if not math.isclose(v1, v2, rel_tol=1e-9, abs_tol=1e-9):
            print(f"  - Diferença em '{path}': {v1} != {v2}")
            return False
    elif v1 != v2:
        print(f"  - Diferença em '{path}': {v1} != {v2}")
        return False
    return True

def compare_dicts(d1, d2, path=""):
    """Compara dois dicionários recursivamente."""
    are_equal = True
    
    keys1 = set(d1.keys())
    keys2 = set(d2.keys())
    
    if keys1 != keys2:
        print(f"  - Diferença de chaves em '{path}':")
        if keys1 - keys2:
            print(f"    - Chaves apenas no primeiro arquivo: {sorted(list(keys1 - keys2))}")
        if keys2 - keys1:
            print(f"    - Chaves apenas no segundo arquivo: {sorted(list(keys2 - keys1))}")
        return False

    for key in sorted(list(keys1)):
        new_path = f"{path}.{key}" if path else key
        
        # Ignora a chave de tempo de execução
        if key == "tempo_execucao_ms":
            continue

        val1 = d1[key]
        val2 = d2[key]

        if isinstance(val1, dict) and isinstance(val2, dict):
            if not compare_dicts(val1, val2, new_path):
                are_equal = False
        elif isinstance(val1, list) and isinstance(val2, list):
            if len(val1) != len(val2):
                print(f"  - Diferença no tamanho da lista em '{new_path}'")
                are_equal = False
            else:
                for i, (item1, item2) in enumerate(zip(val1, val2)):
                    if not compare_values(item1, item2, f"{new_path}[{i}]"):
                        are_equal = False
        else:
            if not compare_values(val1, val2, new_path):
                are_equal = False

    return are_equal

def main():
    if len(sys.argv) != 3:
        print("Uso: python src/compare_results.py <arquivo_referencia.json> <arquivo_a_comparar.json>")
        sys.exit(1)

    file1_path = sys.argv[1]
    file2_path = sys.argv[2]

    print(f"Comparando:\n  1: {file1_path}\n  2: {file2_path}\n")

    try:
        with open(file1_path, 'r') as f1:
            data1 = json.load(f1)
        with open(file2_path, 'r') as f2:
            data2 = json.load(f2)
    except FileNotFoundError as e:
        print(f"Erro: Arquivo não encontrado - {e.filename}")
        sys.exit(1)
    except json.JSONDecodeError:
        print("Erro: Falha ao decodificar um dos arquivos JSON. Verifique se o formato está correto.")
        sys.exit(1)

    if compare_dicts(data1, data2):
        print("✅ SUCESSO: Os resultados são consistentes!")
    else:
        print("\n❌ FALHA: Foram encontradas diferenças nos resultados.")

if __name__ == "__main__":
    main()