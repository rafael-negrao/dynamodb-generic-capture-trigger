# dynamodb-generic-capture-trigger

Capturador genérico de dados tendo como origem o DynamoDB e destino Kafka

## Requisitos

Seguem os requisitos para rodar todos os itens deste projeto local na máquina:

- python: 3.8 ou superior
- docker: 24.0.2 ou superior
- docker compose: v2.19.1 ou superior
- aws-cli
- poetry
- make
- terraform

## Estrura do Projeto

| Diretório                                | Descrição                                                                       | 
|------------------------------------------|---------------------------------------------------------------------------------|
| **[src](src)**                           | Fontes da aplicação lambda function                                             |
| **[tests](tests)**                       | Fontes dos testes unitários                                                     |
| **[environment](environment)**           | Contém os arquivos referente ao docker e terraforma para desenvolvimento local  |
| **[integration_test](integration_test)** | Fontes para os testes integrados utilizando o ambiente de desenvolvimento local |

## Makefile

- Como executar:

```
make nome_da_funcao 
```

- Funções implementadas para empacotamento e testes

| Função                  | Descrição                                                                                                                    | Função predecessora que será executada |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| **clean**               | Excluí os arquivos do diretório de distribuíção                                                                              | Não se aplica                          |
| **build**               | Atualiza as bibliotecas e compila o projeto                                                                                  | Executa a função **clean**             |
| **package**             | Faz o empacotamento do projeto concentrando os arquivos no diretorio target                                                  | Executa a função **build**             |
| **test**                | Executa os testes planejados no projeto                                                                                      | Executa a função **package**           |
| **integration-test**    | Executa os testes integrados para o ambiente de desenvolvimento local. **Observar as instruções básicas de uso**             | Executa a função **package**           |
| **install**             | Executa a instalação do projeto conforme parametrização especificada no arquivo `pyproject.toml` (config do poetry)          | Executa a função **test**              |
| **export-requirements** | Faz a criação do arquivo `requirements.txt` usando a paramentrização realizada no aquivo `pyproject.toml` (config do poetry) | Não se aplica                          |

- Funções para subir o ambiente de desenvolvimento local

| Função                            | Descrição                                                     | Função predecessora que será executada            |
|-----------------------------------|---------------------------------------------------------------|---------------------------------------------------|
| **env-local-dev-stop**            | Faz o stop do docker compose do projeto                       | Não se aplica                                     |
| **env-local-dev-start**           | Faz o start do docker compose do projeto                      | Executa a função **env-local-dev-stop**           |
| **env-local-dev-terraform-init**  | Faz a inicialização dos módulos do terraform                  | Não se aplica                                     |
| **env-local-dev-terraform-plan**  | Faz a planejamento do que será provisionado no ambiente local | Executa a função **env-local-dev-terraform-init** |
| **env-local-dev-terraform-apply** | Faz o deploy local do que foi provisionado no ambiente local  | Executa a função **env-local-dev-terraform-plan** |

## Ambiente de desenvolvimento local

O ambiente de desenvolvimento local:

- docker-compose, serviços:
    - **Kakfa**
        - confluentinc/cp-zookeeper:7.4.1.arm64
        - confluentinc/cp-kafka:7.4.1.arm64
        - quay.io/cloudhut/kowl: Permite visualizar as mensagens publicadas no Kafka
    - **Localstack**
        - localstack/localstack: Emula o ambiente da aws local na máquina, serviços utilizados
            - IAM
            - Lambda Function
            - DynamoDB
            - DynamoStream
        - Limitações
            - Não está implementado o serviço de **TTL** do **DynamoDB**
            - Esta versão gratuíta do localstack **não valida IAM**

### Instruções básicas para executar os testes unitários

Seguem os passos:

1. Abrir o terminal

2. Subir o docker na máquina

```shell
make test
```

### Instruções básicas de uso para subir o ambiente

Seguem os passos:

1. Abrir o terminal

2. Subir o docker na máquina

```shell
make env-local-dev-start
```

3. Após execução do item 2 e aguardar que ele já tenha subido por completo, aplicar o terraform planejado na máquina

```shell
make env-local-dev-terraform-apply
```


### Instruções básicas para executar os testes integrados

#### Requisitos
- Ter executado **[Instruções básicas de uso para subir o ambiente]**

Seguem os passos:

1. Abrir o terminal

2. Subir o docker na máquina

```shell
make integration-test
```