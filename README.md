# Pipeline Apoio Técnico — 2026

Automação ETL para ler dados de três abas do Google Sheets, comparar com a tabela `public.apoio_tecnico` no Supabase e inserir somente registros ausentes referentes a 2026.

## Escopo

- Este projeto atende exclusivamente ao fluxo de atualização e conferência dos dados de 2026.
- A comparação utiliza a chave de negócio de sete campos: Categoria, Evento, Município, Data de Realização, Gerência, Modalidade do Apoio e Tipo de Apoio.
- O pipeline não atualiza nem exclui registros existentes.
- O processo é interrompido se alguma das três abas necessárias não puder ser lida.
- A execução exige uma chave secreta do Supabase (`sb_secret_`). Não utilize chave publicável.

## Estrutura de arquivos

```text
pipeline-2026/
├── pipeline_atm.py
├── requirements.txt
├── README.md
├── .env.example
├── .gitignore
├── .env                 # criar localmente; não versionar
├── credenciais.json     # obter/criar localmente; não versionar
├── pipeline.log         # gerado durante a execução
└── relatorios/          # gerada durante a execução
```

## Requisitos

- Python 3.10 ou superior recomendado.
- Acesso de leitura à planilha Google configurado por uma conta de serviço.
- Projeto Supabase e chave secreta com permissões necessárias para consultar e inserir na tabela.

## Instalação

Abra o PowerShell na pasta do projeto e execute:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
```

Se o PowerShell bloquear a ativação do ambiente virtual, pode instalar as dependências sem ativá-lo usando `py -m pip install -r requirements.txt`.

## Configuração local

1. Faça uma cópia de `.env.example` com o nome `.env`.
2. Preencha `SUPABASE_URL` com a URL do projeto.
3. Preencha `SUPABASE_SERVICE_ROLE_KEY` com a chave secreta `sb_secret_` do Supabase.
4. Coloque o arquivo de credenciais da conta de serviço Google com o nome `credenciais.json` na mesma pasta de `pipeline_atm.py`.
5. Confirme que a conta de serviço tem acesso de leitura à planilha configurada no código.

**Segurança:** nunca compartilhe nem faça commit de `.env` ou `credenciais.json`. O `.gitignore` deste projeto já os exclui do Git.

## Execução

Primeiro, execute a simulação para conferir a comparação sem gravar dados:

```powershell
py pipeline_atm.py --dry-run
```

Para executar o pipeline no ano padrão (2026), após revisar a simulação:

```powershell
py pipeline_atm.py --ano 2026
```

O pipeline possui limite padrão de inserções e opções adicionais. Para consultar todas as opções:

```powershell
py pipeline_atm.py --help
```

## Saídas

- Relatório Excel na pasta `relatorios/`.
- Log de execução em `pipeline.log`.
- Arquivo de duplicidades da planilha, quando gerado pelo processo.

Esses arquivos são resultados locais e não devem ser enviados ao repositório por padrão.

## Histórico de versões

- `v1.0.0` — primeira versão consolidada do pipeline de 2026, após conferência integral entre a planilha e o Supabase.
