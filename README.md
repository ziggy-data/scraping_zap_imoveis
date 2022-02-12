# scrapping Zap Imoveis
Neste projeto estou fazendo web scrapping de apartamentos em todo o Brasil no site Zap-Imoveis, ETL, análise de dados e dashboard. 

O notebook "Zap-imoveis" é responsável pelo scrapping de dados. Neste caso ele está especializado em fazer 4 diferentes dados, sendo: 

* Uma amostra de dados geral de casas no Brasil
* Uma amostra de dados geral de apartamentos no Brasil  
* Uma amostra de dados de casas no Rio de Janeiro  
* Uma amostra de dados geral de apartamentos no Brasil

## Explicação dos notebooks:

### Notebook: Zap-imoveis
Esse notebook possui 2 tipos de tratamento de dados, sendo:

Realiza scrapping de 400 páginas (aceita alterações), possui tratamento de exceções com erro de url ou site inacessível, utilizando 3 tipos de links diferentes com diferentes facilidades de acesso e uma extração de informação de parte da descrição com Regular Expression. 

* Todos os scrapy possuem tratamentos de RegEx diferentes seguindo a demanda de cada dataset.
* As duas primeiras amostras de dados não possui distinção entre uma amostra máxima ou mínima de especificações.
* A penultima possui um tratamento com a distinção dos imóveis e também uma redundância se não ocorrer uma distinção .

### Notebook: regressao-linear

Neste notebook estou aplicando tecnicas de regressão linear com o dataset: `dataset-casas-rj.csv`

O modelo apresentou um acerto de 43% em relação ao preços dos imóveis considerando todas as variáveis numéricas presentes nesse dataset.

É possivel fazer melhorias futuras em relação a taxa de acerto. 

## Detalhe das amostras:

* `dataset-casas-geral.csv` : possui registros de 9600 casas no território brasileiro.
* `dataset-ap-geral.csv` : possui registros de 8400 apartamentos no território brasileiro.
* `dataset-casas-rj.csv` : possui registros de 9600 casas no território carioca.

(Os dados neste repositório foram armazenados em 01/02/2022)

## Dashboards:

### Dashboard RJ  -  Apartamentos:

Neste projeto estou estudando dados de apartamentos que estão à venda pelo site Zap imóveis no momento da granulação e assim facilitando o acesso aos dados do site e para análise. 


Ele nos traz dados de valor imobiliário em maioria das ruas no Rio De Janeiro e assim permitindo analisar.
 
Alguns itens usados atráves de web scrapping: Tipo de venda, Área, Bairro, Rua, Condomínio, IPTU, Quartos, Vagas, Valor, Banheiros

Dados extraídos: Cidade e valor por m² 

* Métricas utilizadas:

 ```
valor por m² = "R$ " & round(sum([Valor]) / sum([Área (m²)]),2)
 ```
 
 ```
 atualizacao = "Atualizado em " & SELECTEDVALUE('Atualização'[Data e hora])
 ```

## Link para acessar o Dashboard:
https://app.powerbi.com/reportEmbed?reportId=cd532ea4-be16-4822-8477-f18724660c65&autoAuth=true&ctid=da49a844-e2e3-40af-86a6-c3819d704f49&config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly93YWJpLWJyYXppbC1zb3V0aC1yZWRpcmVjdC5hbmFseXNpcy53aW5kb3dzLm5ldC8ifQ%3D%3D

 ![Imagem da dashboard](/dashboard/imagem/dashboard-rj.png)

## Link para acessar o dashboard

https://app.powerbi.com/reportEmbed?reportId=6d421255-0e0d-4e0c-adb8-04778dee68fd&autoAuth=true&ctid=da49a844-e2e3-40af-86a6-c3819d704f49&config=eyJjbHVzdGVyVXJsIjoiaHR0cHM6Ly93YWJpLWJyYXppbC1zb3V0aC1yZWRpcmVjdC5hbmFseXNpcy53aW5kb3dzLm5ldC8ifQ%3D%3D

 ![Imagem da dashboard](/dashboard/imagem/dashboard-apt-geral.png)

# Objetivos para fazer nesse projeto:

* Scrapping do zap imoveis :heavy_check_mark:
* Tratamento de dados :heavy_check_mark:
* Extrair informação de texto com RegEx :heavy_check_mark:
* Disponibilizar o conteúdo em: `.csv` :heavy_check_mark:
* Visualizações dos dados em gráficos com seaborn :heavy_check_mark:
* análise estatística de dados :x:
* Dashboards interativas com PowerBI :heavy_check_mark:
* Tentar montar um modelo preditivo de valor dos imóveis :heavy_check_mark:

