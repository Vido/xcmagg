---
title: "O Que Acontece Quando um Ciclista Não Pode Pedalar"
description: "Uma lesão no cotovelo, duas viagens de carro e uma obsessão com equipamentos que virou projeto de comunidade."
lang: pt-br
publish_date: 2026-01-15
updated_date: 2026-01-15
translation_group: how-edcx-started
draft: true
tags: ["edcx", "edc", "indie-hacking", "sqlite", "pessoal"]
author: "Lucas Vido"
---

Novembro de 2025. Luxei o cotovelo na trilha. Sem pedalar por um tempo — ordens médicas.

Parece pouca coisa. Não foi. Pedalo obsessivamente há anos. Meu cérebro funciona com objetos de foco: coisas que pesquiso às 2h da manhã, coisas que acompanho no Mercado Livre, coisas em que penso quando deveria estar fazendo outra coisa. A bike era esse objeto. Sem ela, meu cérebro começou imediatamente a procurar um substituto.

Encontrou um: **EDC**.

## O Buraco do Coelho

EDC — *Every Day Carry*, o que você carrega todo dia. Se você não conhece, pense no r/EDC do Reddit: pessoas postando fotos do que está no bolso. Canivetes, lanternas, multiferramentas, carteiras, canetas. Uma cultura inteira de equipamentos que você realmente usa, todo dia, que merecem estar no seu bolso ou mochila.

Entrei rápido. Buraco do coelho da Victorinox. Guerras de lúmens de lanterna. O debate eterno entre lâmina fixa e folder. O tipo de pesquisa que parece produtiva mas é principalmente divertida.

A lesão forçou algo útil: me obrigou a *usar* o equipamento. Não só pesquisar.

## Duas Viagens

Mais ou menos nessa época, tinha uma viagem de negócios marcada para o Paraguai. O objetivo principal era trabalho — reuniões, o de sempre. Mas o Paraguai tem essa atração gravitacional: lojas duty-free com equipamentos importados a preços que simplesmente não são justos. Saí com uma Victorinox que estava de olho há meses. Dano colateral, me disse.

Depois Pontal do Sul e Ilha do Mel. Alguns dias caminhando pelas trilhas da ilha — sem estradas, sem carros, só barcas. O equipamento importa de verdade ali: o canivete que você pega para preparar a comida no acampamento, a lanterna para navegar na trilha no fim do dia, as camadas que você embalou. É a diferença entre "tenho essa faca" e "confio nessa faca."

As duas viagens fizeram a mesma coisa: transformaram EDC de hobby em hábito.

## A Coceira

De volta em casa, ficava circulando entre Reddit, YouTube e fóruns espalhados. O r/EDC é ótimo — ativo, global, opinativo. Mas tem algumas lacunas que começaram a coçar:

- Sem rastreamento de inventário. Cada post de "o que tem na sua coleção" é uma foto que some no feed.
- Sem catálogo estruturado. Reviews vivem em threads de comentários. Sem sistema de avaliação, sem comparação.
- Os mercados de troca (r/EDCexchange, r/Watchexchange) funcionam, mas são desconectados do contexto da comunidade.
- Quase nada em português. A galera de EDC no Brasil existe, mas está espalhada.

Queria algo que combinasse o clima de comunidade do Reddit com estrutura real: páginas de equipamento com avaliações, loadouts de usuários que você pode navegar e acompanhar, um lugar para anunciar equipamento sem intermediário.

## EDCX

Isso é o EDCX. Não é loja, não é marketplace — é comunidade. Usuários postam pocket dumps, comentam, avaliam equipamentos, montam seu próprio inventário. Quando alguém quer vender ou trocar, posta — o EDCX não intermedia, não cobra nada, não toca na transação. Pense no r/EDCexchange mas conectado à comunidade que realmente se importa com o equipamento.

Ideia simples. A parte divertida foi descobrir como construir isso enxuto.

## As Apostas Técnicas

Três ideias boas o suficiente para realmente me comprometer:

**Arquitetura estrela — Node e NodeKind.**
Em vez de tabelas separadas para usuários, itens de equipamento, posts, coleções e transações, tudo é um `Node`. Um `NodeKind` distingue o que é. Relacionamentos entre nodes são só arestas no mesmo grafo. Parece abstrato, mas na prática significa que posso adicionar um novo tipo de conteúdo sem migração de tabela. O schema é estável; o significado está no kind.

**Magiclink OTT + QR.**
Sem senhas. Você digita seu e-mail, recebe um link de token único. No mobile, o mesmo fluxo funciona via QR code — aponta a câmera, toca, entrou. O onboarding de uma comunidade de equipamentos não deveria parecer preenchimento de declaração de imposto. O caminho sem senha remove atrito exatamente no momento em que novos usuários decidem se ficam.

**SQLite3 do começo ao fim.**
Sem Postgres, sem RDS, sem custo de banco de dados gerenciado. SQLite rodando num bind mount de host, servido pelo Django, com WAL mode e connection pooling. O banco de dados é literalmente um arquivo em disco. Para um app de comunidade em escala inicial, é a escolha certa: custo de infra perto de zero, zero overhead operacional, e o desempenho é melhor do que a maioria das pessoas imagina.

## O Que Vem Agora

O EDCX está sendo construído. Se você curte EDC — ou só quer ver o que um nerd de canivetes com habilidades em Django constrói quando o cotovelo não deixa pedalar — vem encontrar.

O pocket dump espera.
