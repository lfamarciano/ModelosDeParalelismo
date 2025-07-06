# ModelosDeParalelismo
Repositório para o trabalho final da disciplina de Computação Escalável. O objetivo é realizar um experimento comparando diferentes modelos de paralelismo, através da implementação de soluções de um mesmo problema para cada abordagem.

## Guia de execução:

### 1) Requisitos: 

- Docker e Docker Compose instalados
- streamlit instalado localmente
  
 ```
 pip install streamlit pandas matplotlib
 ```

 ### 2) Build das imagens:

 Na raiz do projeto, rode:
 ```
./build_all.sh
 ```

 ### 3) Executar o dashboard
 O dashboard permite gerar os dados e testar as três abordagens de forma sequencial:
  ```
 streamlit run dashboard.py

 ```
 Acesse o endereço exibido no terminal (http://localhost:8501) e preencha:

 - Grau máximo de paralelismo (ex: 1, 2, 4, 8, 16)
 - Número de eventos por estação
 - Número de estações
  
Depois, clique em "Iniciar Experimento".

### 4) Resutados:

O dashboard exibirá:

- Uma tabela com o tempo de execução de cada abordagem e paralelismo
- Um gráfico de desempenho com tempo vs. paralelismo
- Atualização em tempo real à medida que os experimentos terminam
