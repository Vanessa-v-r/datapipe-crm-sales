import os

def generate_markdown(root_dir, output_file='codigo_documentacao.md'):
    with open(output_file, 'w', encoding='utf-8') as md_file:
        for subdir, dirs, files in os.walk(root_dir):
            # Ignorar diretórios indesejados
            dirs[:] = [d for d in dirs if d != '__pycache__']
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(subdir, file)
                    relative_path = os.path.relpath(file_path, root_dir)
                    md_file.write(f'## {relative_path}\n\n')
                    md_file.write('```python\n')
                    with open(file_path, 'r', encoding='utf-8') as f:
                        md_file.write(f.read())
                    md_file.write('\n```\n\n')
    print(f'Documentação gerada no arquivo {output_file}')

if __name__ == '__main__':
    # Substitua 'luigi_pipeline' pelo caminho do diretório raiz do seu projeto
    generate_markdown('luigi_pipeline')
