---
title: "Como o XCMAGG começou? O agregador de provas sem firula"
description: "Por que juntei uma dúzia de plataformas de inscrição espalhadas num único calendário — e por que tudo isso é só um scraper que escreve um arquivo JSONL e uma página HTML estática que o lê."
lang: pt-br
publish_date: 2025-10-02
updated_date: 2025-10-02
translation_group: how-i-started-xcmagg
draft: false
tags: ["xcmagg", "web-scraping", "python", "duckdb", "indie-hacking"]
author: "Lucas Vido"
---

Porque eu resolvi fazer: eu não conseguia achar provas de MTB!
BTW — eu estava na minha fase de hiperfoco em ciclismo.

## Duas dores

**Primeira: os eventos estão espalhados.** Se você pedala no Brasil e quer saber
quais provas estão chegando, não existe um lugar único pra olhar. Cada evento mora
na plataforma de inscrição que o organizador escolheu — e tem mais ou menos uma
dúzia delas. Não existe calendário compartilhado. O calendário das federações não
linka pra página do evento. Muitos organizadores dependem da página no Instagram.
Você descobre uma prova quando um amigo posta no grupo do WhatsApp três dias antes
de fechar a inscrição...

**Segunda: os sites que existem filtram muito mal.** Mesmo nas plataformas que
listam eventos, a geografia é quebrada. Elas filtram pela *ideia delas* de região,
não por onde você de fato está. Eu moro perto de Bragança Paulista, em São Paulo.
Algumas das melhores pedaladas perto de mim ficam no **Sul de Minas** — perto o
suficiente pra ir e voltar num sábado, mas do outro lado da divisa de estado.
Nenhuma ferramenta deixa eu dizer "mostra o que está a uma distância razoável de
carro", porque nenhuma delas sabe *onde* os eventos estão — só em qual dropdown de
estado foram cadastrados.

Resolvi scrapar!

## O que é o XCMAGG

XCMAGG vem de **Cross Country Marathon AGGregator**.
A tarefa é ingrata: visitar toda plataforma de inscrição, extrair todo evento de
ciclismo, normalizar a bagunça num formato limpo só, e produzir um feed único.
Assim eu tenho quando e onde cada prova acontece.

## A tese do "sem firula"

Na semana antes de eu começar esse projeto — um parceiro de negócios recebeu
 uma oferta pra adquirir uma startup, e eu fui chamado pra due diligence.
A coisa estava absurdamente superengenheirada pro estágio do negócio:
cerca de **200 clientes** e por volta de **R$17 mil/mês** de infraestrutura.
O custo escalava com cada novo cliente de um jeito tão bizarro que eles
tinham *parado de pegar clientes* — cada cadastro novo piorava o unit-economics.
Insano, uma empresa recusando receita ativamente porque
a própria arquitetura taxava o crescimento.

É contra esse anti-pattern que o XCMAGG foi pensado. Um exercício: O objetivo fazer
usando a **menor** quantidade de recursos — não a stack mais impressionante. Um
scraper escrevendo um arquivo plano custa mais ou menos o mesmo pra 200 ou pra 200 mil
pessoas. Eficiência de custo aqui não é métrica de vaidade; é o que deixa uma
ferramenta gratuita seguir gratuita e um maker solo seguir lançando.

Aqui está a decisão de arquitetura da qual mais me orgulho, e ela soa como preguiça
até você conviver com ela:

> O produto inteiro é um scraper que escreve um **arquivo JSONL**,
> mais uma **página HTML estática** que o lê.

Sem API. Sem framework de SPA. Sem backend com banco de dados no caminho da
requisição. A página que você abre é um arquivo HTML plano que dá `fetch()` num
único `data.jsonl` e renderiza no navegador.

Essa foi uma decisão *opinionated* contra slop. Muitos devs carregam práticas de
cargo-cult, pré-concebidas. Isso cria complexidade desnecessária — e empaca as
empresas. Temos que lutar contra a complexidade/slop;

## Por trás da construção: Arquitetura Medallion (Bronze → Silver → Gold)

Numa consultoria anterior numa startup — eles tinham uma API plugada num Redis (que
era o banco de persistência). Tinha um ORM caseiro nojento. Os devs não estavam
preparados pra lidar com um NoSQL. Isso era complexo e causava muitos, muitos bugs. A
solução foi cortar a abstração inútil — chamar as primitivas do Redis direto, sem
wrapper, sem abstração — a própria camada de API era a abstração.
Usar a abstração errada custa caro.

Scraping de dados é complexo — às vezes parece caótico.
A decisão crucial/vital é quando/onde aplicar abstrações.
ab -> sufixo que significa 'afastado' ou 'de longe'
tração -> puxar
scraper puxa de longe?

Voltando,  A complexidade mora no pipeline que *produz* o arquivo — não em servi-lo.
O scraper segue uma arquitetura medallion.
Os dados passam por três camadas, ficando mais limpos a cada passo.

### Bronze — raspagem crua

TL;DR -> Como baixar HTML/JSON/PDF pro filesystem.

Bronze é onde a sujeira entra. Cada fonte ganha seu próprio crawler — mais ou menos
uma dúzia de classes, uma por site/api.

Algumas fontes são APIs JSON amigáveis. Algumas são HTML que eu parseio com
BeautifulSoup. Algumas publicam os detalhes do evento em **PDF**, que eu leio com
`pdfplumber`. Umas poucas ficam atrás de proteção anti-bot que rejeita um `requests`
normal, então uso `curl_cffi` pra imitar o fingerprint TLS de um navegador de
verdade.

E as vezes fontes morrem. Minha lista de crawlers tem linhas comentadas — fontes que
fecharam, ou mudaram tanto que não valiam mais a perseguição. Manter um agregador
significa aceitar que suas fontes são jogo de gato e rato.

### Silver — normalização

TL;DR -> Como carregar os dados no DuckDB.

Silver é onde o caos vira estrutura. Dois problemas dominam: **datas** e
**localizações**, e os dois chegam como texto livre escrito por gente.

Datas aparecem como intervalos, dias únicos, nomes de mês, formatos ambíguos.
Localizações são piores — "Serra da Mantiqueira", um nome de local sem cidade, uma
cidade escrita de três jeitos diferentes.

Pros casos genuinamente bagunçados eu apoio em **agentes LLM** (tool-calling da
OpenAI) pra transformar o texto livre num objeto estruturado `{city, uf, ...}` em que
o resto do pipeline pode confiar.

### Gold — publicação

TL;DR -> Como enriquecer e publicar os resultados.

Gold é a recompensa. Uma query `COPY` do DuckDB achata tudo num único `data.jsonl`,
e na saída cada evento é **enriquecido geograficamente**: eu caso a cidade contra uma
base de municípios do IBGE pra anexar um DDD mais latitude e longitude.

Esse passo de geo é a resposta inteira pra coceira número dois. Uma vez que todo
evento tem lat/long de verdade, "o que está perto de Bragança Paulista" — incluindo
provas lá no Sul de Minas — vira um cálculo de distância no navegador, em vez de um
chute contra um dropdown de estado.

Também tem trabalho de dedup aqui. Uma plataforma, por exemplo, manda o mesmo evento
sob várias capitalizações de slug que compartilham o mesmo id no final, então eu
dobro tudo numa URL canônica só. E todo link de saída ganha um `?utm_source=xcmagg`
pra eu enxergar que o agregador de fato manda tráfego pros organizadores.

Uma linha do `data.jsonl` final fica assim:

```json
{"title":"Desafio Speed - Almenara 2026","url":"https://ticketing.example/e/desafio-speed-almenara-2026?utm_source=xcmagg","start_date":"22-08-2026","city":"Almenara","uf":"MG","ddd":"33","latitude":-16.1785,"longitude":-40.6942,"sport":"Corrida de Rua"}
```

Essa única linha — limpa, localizada, atribuída — é a saída inteira de toda aquela
maquinaria. Multiplique por algumas centenas e você tem o calendário.

## Por que esse formato vence pra quem trabalha sozinho

Eu construo isso sozinho. Toda escolha de arquitetura é, no fundo, uma escolha sobre
quanto peso operacional eu topo carregar.

- Eu consigo disparar esse pipeline do meu PC local — custo zero.
- Os custos de LLM ficam no mínimo — o LLM só é chamado nos casos de borda.
- Os custos de servidor são quase zero.
- O deploy é como se fosse os anos 2000.

## O que vem agora

O agregador continua crescendo — mais fontes, filtragem por proximidade mais
inteligente, e eventualmente um calendário público que qualquer um pode abrir pra
achar a próxima prova. A parte difícil está feita: existe um arquivo limpo só que
sabe o que está acontecendo e onde.

E se você é maker — rouba a arquitetura. Um scraper, um arquivo JSONL e uma página
estática te levam mais longe do que você imagina.
