---
title: "Você precisa mesmo de GIS?"
description: "A maioria dos sites filtra localização como categorias aninhadas. Cidades são 2D. Um dataset gratuito do IBGE e uma fórmula do colégio — sem GIS."
lang: pt-br
publish_date: 2026-07-03
updated_date: 2026-07-03
translation_group: do-you-really-need-gis
draft: false
use_katex: true
tags: ["racefeed", "geo", "sqlite", "GIS", "indie-hacking", "yagni"]
author: "Lucas Vido"
---

Existe uma aproximação clássica da engenharia: **π ≈ 3**.
Nada de foguete indo pro espaço, nem voando de ré. Só boa e velha conta de padeiro e um fator de segurança gostoso pra dormir tranquilo.
Quebra o galho — e principalmente — *rápido*.

Nós, engenheiros de software, geralmente não somos tão sofisticados. A maioria nem chega na "Terra Plana" — já seria uma melhoria numérica. A maioria trata geografia como categoria mesmo.

## Cidades como categorias: jeito burocrata

Acesse qualquer site de inscrição de eventos no Brasil e olhe o filtro de localização.<br>
Você recebe um dropdown: **País → Estado → Cidade**.<br>
<a href="https://www.ticketsports.com.br/calendar/filters?city=S%C3%A3o+Paulo&state=SP&country=BR" rel="nofollow">https://www.ticketsports.com.br/calendar/filters?city=São+Paulo&state=SP&country=BR</a> é um bom exemplo.

Esse modelo trata geografia como taxonomia, uma hierarquia burocrática.
Ele falha no instante em que a realidade não respeita essas fronteiras.
Devs adoram... serve certinho numa tabela SQL.

Eu moro perto de Bragança Paulista, interior de São Paulo.
Algumas das melhores provas de MTB perto de mim ficam no **Sul de Minas** (paraíso do MTB), perto o suficiente pra pegar a estrada no sábado de manhã, mas do outro lado da divisa.
Nenhum dropdown vai me mostrar essas provas se eu filtrar "São Paulo".

Falha do lado oposto também. <a href="https://en.wikipedia.org/wiki/Presidente_Prudente" rel="nofollow">Presidente Prudente</a> e <a href="https://en.wikipedia.org/wiki/S%C3%A3o_Jos%C3%A9_do_Rio_Preto" rel="nofollow">São José do Rio Preto</a> são ambas São Paulo. Mas ficam a **+400 km** de distância. Esse filtro que me mostra essas provas misturads com as que eu realmente consigo dirigir... só ruído inútil.

O filtro não sabe onde eu *estou* — ele só sabe em qual gaveta o organizador arquivou a prova.

## O estalo

> Cidades não são categorias. São **pontos numa esfera**.

O XCMAGG já extrai cidade e estado de todo evento que raspa.
Na camada Gold, cada evento é geo-enriquecido via JOIN contra a **base de municípios do IBGE**. Um JSON gratuito e completo com toda cidade brasileira: nome, UF, DDD, latitude, longitude.

```python
LEFT JOIN geo g
    ON strip_accents(LOWER(TRIM(e.location->>'city'))) = strip_accents(LOWER(TRIM(g.nome)))
    AND UPPER(TRIM(e.location->>'uf')) = UPPER(TRIM(g.uf))
```

O `data.jsonl` de saída já tinha `city` e `uf`.
Depois desse JOIN, também tem `latitude` e `longitude`.

Só isso: Um JOIN. Sem extensão GIS. Sem índice espacial. Sem PostGIS.
O banco que eu já precisava pra buscar cidades me deu coordenadas 2D de graça.

## Três níveis de "perto o suficiente"

Com lat/lon em cada evento, "o que tem perto de mim" vira um problema de matemática.
Três opções, em sofisticação crescente:

**1. Bounding box** — bem grosseiro:

$$|\phi_2 - \phi_1| < \delta \quad \text{e} \quad |\lambda_2 - \lambda_1| < \delta$$

Funciona. Péssimo perto dos polos (o Brasil tem muito problemas, mas esse não é um deles).
Bom o bastante pra um protótipo - ou até mais qe isso.

**2. Distância pitagórica** — que ensinamos ao adolescentes:

$$d = \sqrt{(\phi_2-\phi_1)^2 + (\lambda_2-\lambda_1)^2}$$

Modelo preferido dos terraplanistas. Degrada em distâncias grandes.
Pra "provas num raio de 150 km da minha cidade" no Sudeste, é perfeitamente suficiente.

**3. Haversine** — o modelo esférico completo:

$$d = 2R \arcsin\!\left(\sqrt{\sin^2\!\frac{\Delta\phi}{2} + \cos\phi_1\cos\phi_2\sin^2\!\frac{\Delta\lambda}{2}}\right)$$

Distância real numa esfera. Ainda é só $\arcsin$ e $\sqrt{\phantom{x}}$ — trigonometria pão-com-ovo.
E o SQLite lida com isso nativamente, sem extensões:

```sql
CAST(ROUND(6371 * 2 * asin(sqrt(
    power(sin((radians(g.latitude)  - radians(?)) / 2), 2) +
    cos(radians(?)) * cos(radians(g.latitude)) *
    power(sin((radians(g.longitude) - radians(?)) / 2), 2)
))) AS INTEGER) AS dist_km
```

Mesma fórmula, dois runtimes: SQLite no servidor pra API Django, JS no navegador pro calendário estático.
Nenhum GIS em lugar nenhum.

Escolhei o Haversine. Não porque era necessário. Pitagórico sobreviveria numa boa.
É porque não custou nada a mais e é o modelo certo.
$\pi = 3.14159$, não 3. Aqui dá pra bancar as casas decimais extras.

## Localização sem fricção

O próximo passo natural é perguntar pro usuário onde ele está.
"Permitir localização" — o prompt do navegador.<br>
Invasivo. Lento. Metade dos usuários dispensa no reflexo.

Melhor: **GeoIP**. Casar o IP do usuário contra uma base geolocalizada por cidade.
Não é preciso até a rua... mas eu não preciso do seu endereço - eu não sou a polícia.
Eu preciso saber que ele provavelmente está em Campinas, não em Porto Alegre.
Já basta pra pré-filtrar o calendário e mostrar os eventos relevantes antes de qualquer clique.

Zero fricção. Sem prompt de permissão. E o mesmo sinal deixa o resto do site mais afiado (se eu sei que você anda de bike em Minas, recomendações de equipamento e ofertas de prova já saem local, em vez de ruído genérico).

## Quando você realmente precisa de GIS

Uma stack GIS de verdade — PostGIS, QGIS, R-Trees, projeções elipsoidais — é uma arma poderosa.<br>
Existem alvos legítimos pra ela:

- Consultas de polígono: "esse ponto está dentro dessa fronteira administrativa?"
- Roteamento: caminho mais curto numa malha viária
- Análise de raster: modelos de elevação, cobertura do solo
- Milhões de consultas espaciais por segundo com latência sub-milissegundo

Pra "me mostra provas de MTB num raio de 200 km" — é matar mosquito com fuzil.<br>
E além do exagero, tem um problema de habilidade: a maioria dos devs não familizarizada com GIS.
É uma disciplina à parte, com ferramental, projeções e modos de falha próprios.
Usar isso como padrão adiciona peso operacional e curva de aprendizado para benefício zero.

O caminho da minima-ação: **JSON do IBGE + Haversine + SQLite**.
Grátis. Sataless. Sai do forno na hora do almoço.

## O que isso destravou

O calendário estático em [racefeed.com.br/events](https://racefeed.com.br/events/) agora tem:

- **Perto de mim** → um clique, filtra eventos dentro de um raio configurável
- **Ordenar por distância** → prova mais perto primeiro, não só a mais próxima na data
- **~X km** exibido em cada card quando "perto de mim" está ativo

Nada disso precisou de banco espacial.
Precisou tratar cidades como o que elas já são:<br>
> dois pontos num sistema de coordenadas