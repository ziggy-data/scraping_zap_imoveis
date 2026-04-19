# 🏠 Health Check — Dataset de Imóveis RJ

**Arquivo:** `DATASET_RIO_BI_READY.csv`  
**Data:** 19/04/2026 07:31  
**Registros:** 2842 | **Colunas:** 215

## 📊 Resumo Geral

**Geolocalização:**

- Site ZAP: 1873 (65.9%)
- Nominatim (Endereco): 913 (32.1%)
- Nominatim (Bairro): 52 (1.8%)
- Nao Encontrado: 2 (0.1%)
- Nominatim (Rua): 2 (0.1%)

**Tipos de imóvel:**

- Apartamento: 1936
- Sala / Conjunto: 258
- Cobertura: 165
- Casa: 143
- Casa De Condomínio: 125
- Ponto Comercial / Loja / Box: 74
- Casa De Vila: 41
- Lote / Terreno: 32
- Kitnet / Conjugado: 15
- Imóvel Comercial: 10
- Flat: 9
- Sobrado: 6
- Duplex: 6
- Prédio / Edificio Inteiro: 6
- Galpão / Depósito / Armazém: 5
- Studio: 2
- Prédio / Edifício Inteiro: 2
- Térrea: 2
- Consultório: 1
- Edifício Residencial: 1
- Triplex: 1
- Não Informado: 1
- Andar / Laje Corporativa: 1

**Status:**

- Pronto: 2802
- Em construção: 18
- Na planta: 18
- Pronto para morar: 4

## 🔍 Validação de Campos Críticos

| Status | Campo | Descrição | Cobertura | Detalhe |
|:------:|-------|-----------|:---------:|---------|
| ✅ | `valor_R$` | Preço do imóvel | 100.0% | 2842/2842 positivos, min=52175, média=971694, max=26000000 |
| ✅ | `area_m2` | Área em m² | 100.0% | 2842/2842 positivos, min=18, média=117, max=7400 |
| ✅ | `condominio_R$` | Valor do condomínio | 83.1% | 2361/2842 positivos, min=0, média=1695, max=2200000 |
| ✅ | `iptu_R$` | Valor do IPTU | 70.4% | 2002/2842 positivos, min=0, média=1136, max=42430 |
| ✅ | `quartos` | Número de quartos | 87.1% | 2474/2842 positivos, min=0, média=2, max=18 |
| ✅ | `bairro` | Bairro do imóvel | 100.0% | 2842/2842 preenchidos. Top: Copacabana(434), Recreio Dos Bandeirantes(273), Tijuca(185) |
| ✅ | `rua` | Rua do imóvel | 99.5% | 2829/2842 preenchidos. Top: Avenida Nossa Senhora De Copacabana(117), Rua Barata Ribeiro(53), Estrada Dos Bandeirantes(43) |
| ✅ | `descricao` | Descrição do anúncio | 99.7% | 2833/2842 com >50 chars, média=786 chars |
| ✅ | `tipo_imovel` | Tipo (Apartamento, Casa...) | 100.0% | 2841/2842 preenchidos. Top: Apartamento(1936), Sala / Conjunto(258), Cobertura(165) |
| ✅ | `status_construcao` | Status (Pronto, Em construção) | 100.0% | 2842/2842 preenchidos. Top: Pronto(2802), Em construção(18), Na planta(18) |
| ✅ | `endereco_completo` | Endereço completo | 100.0% | 2841/2842 preenchidos. Top: Avenida Nossa Senhora de Copacabana - Copacabana, Rio de Janeiro - RJ(34), Rua Barata Ribeiro - Copacabana, Rio de Janeiro - RJ(20), Rua Visconde de Pirajá - Ipanema, Rio de Janeiro - RJ(17) |
| ✅ | `corretora` | Nome da corretora | 98.6% | 2801/2842 preenchidos. Top: Rjc Imobiliária(123), Styllus Imobiliaria Cachambi(105), Família Bacellar Imobiliária - Tijuca(91) |
| ✅ | `coordenadas` | Coordenadas lat/lon | 99.9% | 2840/2842 com coordenadas válidas |
| ✅ | `publicacao_texto` | Texto de publicação | 100.0% | 2841/2842 preenchidos. Top: Publicado há 1 ano, atualizado há 1 dia.(116), Publicado há 4 dias, atualizado há 17 horas.(84), Publicado há 1 semana, atualizado há 14 horas.(80) |
| ✅ | `dias_publicado` | Dias desde publicação | 99.8% | 2836/2842 positivos, min=0, média=219, max=3285 |
| ✅ | `tem_portaria_24h` | Portaria 24h | 42.0% | 1194/2842 = True. ✓ Variação OK |
| ✅ | `tem_elevador` | Elevador | 52.5% | 1491/2842 = True. ✓ Variação OK |
| ✅ | `tem_piscina` | Piscina | 27.2% | 774/2842 = True. ✓ Variação OK |
| ✅ | `preco_m2` | Preço por m² | 100.0% | 2084 valores distintos, min=938.8, média=8497.5, max=32873.9 |
| ✅ | `custo_fixo_mensal` | Custo fixo mensal | 88.0% | 1953 valores distintos, min=0.0, média=1789.7, max=2200037.5 |
| ✅ | `score_conforto` | Score de conforto | 95.4% | 92 valores distintos, min=0.0, média=35.2, max=92.0 |
| ✅ | `score_investimento` | Score de investimento | 100.0% | 10 valores distintos, min=1.0, média=5.7, max=10.0 |
| ✅ | `anos_payback` | Payback (anos) | 100.0% | 543 valores distintos, min=19.4, média=76.5, max=13808.9 |

## 🚨 Colunas Sempre com Mesmo Valor

| Coluna | Valor Fixo |
|--------|:----------:|
| `cidade` | `Rio De Janeiro` |
| `nota_media` | `0.0` |
| `total_avaliacoes` | `0` |
| `tem_armario_embutido` | `0` |
| `tem_interfone` | `0` |
| `merc_faixa` | `0.0` |
| `merc_elegivel` | `0.0` |
| `merc_teto` | `0.0` |
| `merc_subsidio` | `0.0` |
| `merc_economia` | `0.0` |
| `yield_bruto_anual` | `5.4` |
| `alerta_oferta_rua` | `0` |
| `ml_r2_score` | `0.776` |

## 💰 Diagnóstico Financeiro

- **Custo Fixo**: ✅ min=R$0 | média=R$1,790 | max=R$2,200,038
- **Aluguel Estimado**: ✅ min=R$235 | média=R$4,373 | max=R$117,000
- **Fluxo de Caixa**: ✅ min=R$-2,193,355 | média=R$2,583 | max=R$114,800
- **Payback**: ✅ min=R$19 | média=R$76 | max=R$13,809

## 🎯 Scores

- **Conforto (0-100)**: min=0 | média=35.2 | max=92 (92 valores)
- **Investimento (0-10)**: min=1 | média=5.7 | max=10 (10 valores)
- **Mobilidade (0-10)**: min=0 | média=6.9 | max=10 (28 valores)
- **Segurança (0-10)**: min=1 | média=5.1 | max=8 (11 valores)
- **Lifestyle (0-10)**: min=0 | média=4.1 | max=10 (7 valores)

## 📋 Veredito

### ✅ APROVADO — 23/23 campos OK

