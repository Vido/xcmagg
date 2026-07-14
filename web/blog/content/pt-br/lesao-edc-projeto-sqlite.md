---
title: "Quando um Ciclista Não Pode Pedalar"
description: "Uma lesão no cotovelo e uma nova obsessão se transformaram em provas de conceito."
lang: pt-br
publish_date: 2026-01-15
updated_date: 2026-01-15
translation_group: injury-edc-sideproject-sqlite
draft: false
tags: ["edcx", "edc", "indie-hacking", "sqlite", "memcached", "pessoal"]
author: "Lucas Vido"
---

Novembro de 2025. Compressão do nervo do cotovelo numa viagem de bike de vários dias — síndrome do túnel cubital. Sem pedalar por um tempo — ordens do fisio.

Parece pouca coisa, mas não foi. Tenho obsessção por ciclismo há anos. Meu cérebro funciona com objetos de foco: coisas que pesquiso às 2h da manhã, coisas em que penso quando deveria estar fazendo outra coisa. A bike era esse objeto. Sem ela, meu cérebro começou imediatamente a procurar uma *surrogate activities* substituta.

Encontrou um: **EDC**.

## O Buraco do Coelho

EDC — *Every Day Carry*, coisas que você carrega todo dia. Se você não conhece, pense no r/EDC do Reddit: pessoas postando fotos do que está no bolso. Canivetes, lanternas, multiferramentas, carteiras, canetas. Uma cultura inteira de equipamentos que você realmente usa, todo dia, que merecem estar no seu bolso ou mochila.

Me afundei rápidamente neste buraco. Especificação de lanterna — lumens, CRI, throw vs flood, tint, modos de driver, curvas de runtime. Debates eternos sobre aço — D2 vs VG-10, MagnaCut vs S35VN, e se CPM-20CV vale o preço. O tipo de pesquisa que parece produtiva mas é principalmente diversão.

Depois de um tempo percebi que EDC é igual ao TikTok — sequestra seu cérebro e devolve calorias informacionais vazias. <a href="https://www.youtube.com/watch?v=-3-5lbzaQdU" rel="nofollow">Suporte emocional para homens</a>. Me senti muito culpado gastando tanto foco numa armadilha.

## Reddit Não Basta

Fiquei circulando entre Reddit, YouTube e grupos de Telegram. O r/EDC é ótimo — bem ativo, fotos boas.
Percebi que EDC tem dinâmica parecida com a comunidade de relógios — tem produto de ponta, com mercado de revenda ativo.

Mas percebi algumas lacunas:

- Os mercados de troca (r/EDCexchange, r/Watchexchange) funcionam, mas são desconectados do contexto da comunidade.
- Sem rastreamento de inventário. Cada post de "o que tem na sua coleção" é uma foto que some no feed.
- Sem catálogo estruturado. Reviews vivem em threads de comentários. Sem sistema de avaliação, sem comparação.

Queria algo que combinasse o clima de comunidade do Reddit com estrutura real:
páginas de equipamento com avaliações, loadouts de usuários que você pode navegar e acompanhar, um lugar para anunciar equipamento sem intermediário.

## EDCX

Não preciso esconder: me senti muito otário gastando tanto foco e dinheiro em EDC.
Meu raciocínio foi: quero algo útil no final desse processo — redirecionar essa energia pra um canal produtivo.
É isso o EDCX. Não é loja, não é marketplace — é comunidade. Usuários postam pocket dumps, comentam, avaliam equipamentos, montam seu próprio inventário.
Quando alguém quer vender ou trocar, posta — o EDCX não intermedia, não cobra nada, não toca na transação. Pense no r/EDCexchange mas conectado à comunidade que realmente se importa com o equipamento.

Ideia simples. Construir isso enxuto foi o exercício mental que meu cérebro TOC precisa.

## As Apostas Técnicas

Três apostas que eu bancaria:

### SQLite3 do começo ao fim

Sem Postgres, sem RDS, sem custo de banco gerenciado. SQLite rodando num bind mount de host, servido pelo Django.
O banco de dados é literalmente um arquivo em disco. Para um site em estágio inicial, é a escolha certa: custo de infra perto de zero, zero overhead operacional. Backup é só um `scp`.
Performance é imbatível — algo que a maioria das pessoas não imaginaria.

Três PRAGMAs que destravam a maior parte do teto de performance do SQLite:

```sql
PRAGMA journal_mode = WAL;      -- escritores não bloqueiam leitores
PRAGMA mmap_size = 268435456;   -- 256 MB de I/O mapeado em memória, leituras direto do page cache
PRAGMA synchronous = 1;         -- NORMAL: fsync nos checkpoints, não a cada commit
```

WAL (Write-Ahead Log) inverte o modelo de lock padrão: leitores e o único escritor rodam em paralelo em vez de entrar em fila. `mmap_size` diz ao SQLite pra mapear o arquivo do banco direto na memória do processo — leituras pulam o overhead de syscall inteiramente. `synchronous = NORMAL` troca uma fração de durabilidade (segura sob WAL — só uma queda de energia bem no checkpoint perde um commit) por muito menos chamadas de fsync. Juntos transformam um banco "que é só um arquivo" em algo que aguenta tráfego concorrente real sem suar a camisa. <a href="https://www.youtube.com/watch?v=yTicYJDT1zE" rel="nofollow">DjangoCon Europe 2023 — Use SQLite in production</a>, de Tom Dyson, dá um ótimo panorama.

### Arquitetura estrela — Node e NodeKind

Em vez de tabelas separadas para usuários, itens de equipamento, posts, coleções e transações, tudo é um `Node`. Um `NodeKind` distingue o que é. Relacionamentos entre nodes são só arestas no mesmo grafo. Tudo é um Node. Node cuida da identidade — shortcodes, slugs, timestamps — e toda entidade satélite se conecta a ele: Fotos, Comentários, Votos, Transações. Novo tipo de conteúdo? Novo `NodeKind`, zero migração. Muitos projetos caem num atoleiro porque o schema cresce sem um modelo conceitual forte por trás — isso evita isso.

### Magiclink OTT + QR

Autenticação sem senha. Usuário recebe um link de token único por e-mail — sem senha pra esquecer, sem conta pra recuperar.

O efeito colateral maravilhoso é uma **ponte de autenticação pro mobile**: escaneia o QR code no desktop, aponta a câmera no celular, toca — entrou. Onboarding de uma comunidade de equipamentos não deveria parecer preenchimento de declaração de imposto. O caminho sem senha remove atrito bem no momento em que novos usuários decidem se ficam.

Overhead de segurança? Invalidação de token, proteção contra replay — as preocupações de sempre. Resposta simples: guardar os tokens num cache backend (Memcached aqui). Cache já tem TTL por natureza, então tokens efêmeros expiram sozinhos. Sem job de limpeza, sem coluna `used_at`, sem cemitério de token velho no banco.

A mesma instância de Memcached também serve de session backend do Django (`cached_db`) — leitura rápida do cache, escrita durável no SQLite como fallback. Memcached não é puxadinho — é o núcleo de como a autenticação funciona.

## O Que Vem Agora

O EDCX está sendo construído: uma válvula de escape pra teses técnicas, uma válvula de escape pro meu foco obsessivo.
Se você curte EDC — ou só quer ver o que um nerd de canivetes com habilidades em Django constrói quando o cotovelo não deixa pedalar — vem encontrar.


*PS: <a href="https://www.youtube.com/watch?v=HcMitJpkNnM" rel="nofollow">A alfinetada do EDC também funciona em ciclistas.</a>.*
