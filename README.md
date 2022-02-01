# scrapping Zap Imoveis
Neste projeto estou fazendo web scrapping de apartamentos em todo o Brasil no site Zap-Imoveis, ETL, análise de dados e dashboard. 

O notebook "Zap-imoveis" é responsável pelo scrapping de dados. Neste caso ele está especializado em fazer 3 diferentes dados, sendo: 

* Uma amostra de dados geral de casas no Brasil
* Uma amostra de dados geral de apartamentos no Brasil  
* Uma amostra de dados de casas no Rio de Janeiro  

## Explicação dos notebooks:

### Notebook: Zap-imoveis
Esse notebook possui 2 tipos de tratamento de dados, sendo:

Realiza scrapping de 400 páginas (aceita alterações), possui tratamento de exceções com erro de url ou site inacessível e utilizando 3 tipos de links diferentes com diferentes facilidades de acesso. 

* As duas primeiras amostras de dados não possui distinção entre uma amostra máxima ou mínima de especificações.
* A ultima possui um tratamento com a distinção dos imóveis e também uma redundância se não ocorrer uma distinção .

### Notebook: regressao-linear

Neste notebook estou aplicando tecnicas de regressão linear com o dataset: `dataset-casas-rj.csv`

O modelo apresentou um acerto de 43% em relação ao preços dos imóveis considerando todas as variáveis numéricas presentes nesse dataset.

É possivel fazer melhorias futuras em relação a taxa de acerto. 

## Detalhe das amostras:

* `dataset-casas-geral.csv` : possui registros de 9600 casas no território brasileiro.
* `dataset-ap-geral.csv` : possui registros de 8400 apartamentos no território brasileiro.
* `dataset-casas-rj.csv` : possui registros de 9600 casas no território carioca.


# Objetivos para fazer nesse projeto:

* Scrapping do zap imoveis :heavy_check_mark:
* Tratamento de dados :heavy_check_mark:
* Disponibilizar o conteúdo em: `.csv` :heavy_check_mark:
* Visualizações dos dados em gráficos com seaborn :x:
* análise estatística de dados :x:
* Dashboard interativa com PowerBI :x:
* Praticar o conteúdo em Spark :x:
* Tentar montar um modelo preditivo de valor dos imóveis :heavy_check_mark:
* Na espera de mais ideias para implementar :x:
