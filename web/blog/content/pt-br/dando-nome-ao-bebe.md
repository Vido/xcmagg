---

title: "Dando Nome ao Bebê"
description: "Em muitas culturas os bebês não recebem nomes verdadeiros. Isso é feito para evitar azar ou maus espíritos. Quando um bebê ganha um DNS de verdade — é aí começa a brincadeira."
lang: pt-br
publish_date: 2025-11-17
updated_date: 2025-11-17
translation_group: naming-the-baby
draft: false
tags: ["racefeed", "DNS", "python", "SEO", "Google Analytics", "indie-hacking"]
author: "Lucas Vido"
--------------------

Já faz cerca de um mês desde que a primeira versão entrou no ar.
Ela subiui com este nome de bebê — em um subdomínio do meu [site pessoal](https://lvido.tech).

O nome de bebê era *xcmagg.lvido.tech*. Os maus espíritos não fizeram nada com ele.

Os Ainu do Japão acreditavam que o mundo era habitado por *kamuy* (espíritos ou divindades). Como a mortalidade infantil era historicamente elevada, eles desenvolveram uma prática engenhosa de nomenclatura para enganar, afastar e desencorajar os espíritos malignos responsáveis por doenças e infortúnios. Recém-nascidos recebiam inicialmente nomes temporários, desagradáveis ou até vulgares (como *Osoma*, que significa "cocô"). Se uma criança tivesse um nome repulsivo, os espíritos assumiriam que ela era sem valor ou indesejada e a deixariam em paz.

## Azar na bike

Em outubro passado fiz o maior pedal do ano:
Caminho da [Divina Providência](https://www.youtube.com/watch?v=d10chWWTDBc).
Uma viagem de bikepacking de 03 dias, 325 km e 6.730 metros de altimetria acumulada.
Foi um pedal épico — e terminei com uma lesão no cotovelo.
Agora estou em reabilitação, então não devo pedalar novamente este ano.
O lado positivo: mais tempo na frente do terminal.

E não estou mais sozinho. [Adan Marques](https://github.com/Adan-Marques) também está trabalhando neste projeto.

## Recebendo conselhos do Reddit

O Reddit é um lugar cheio de espíritos de porco: bots, spammers, cripto-golpistas e afins...
Mas, de vez em quando, [alguém escreve algo realmente útil](https://www.reddit.com/r/saasbuild/comments/1oj22nx/how_to_scale_your_saas_to_10k_did_it_twice/).

**TL;DR:**

* Não escreva código.
* Crie landing pages e encontre pessoas interessadas primeiro.
* Valide a demanda, não o produto.
* Se encontrar demanda, construa um MVP.
* Coloque-o na frente das pessoas o mais rápido possível (em privado).

Bons conselhos que qrovavelmente eu deveria tê-los seguidos. Mas já havia pulado os passos 1 e 2.
Mostrei o XCMAGG para alguns amigos e eles gostaram. Um deles até se inscreveu na newsletter.
Essa estratégia baseada em newsletter parece um conselho antigo. Talvez já seja tarde demais.
Mas ainda posso trabalhar nos passos 3 e 5, não sei...

MAs se vamos publicar o link na internet, não podemos continuar usando o nome de bebê.
Isso é péssimo para SEO. O XCMAGG precisa de um domínio de verdade.

## RaceFeed

Depois de procurar um domínio interessante e criativo — algo como *xcm.gg*
o ChatGPT apresentou bons argumentos contra domínios do tipo "sopa de letrinhas".

Acabei chegando em *racefeed.com.br*:

> Race — faz referência ao XCM ou às corridas em geral
> Feed — faz referência ao recurso de doomscroll chamado "Feed"

Além disso, *feed* também pode remeter à nutrição (ou hidratação?), que são aspectos essenciais dos esportes de endurance.

Estou satisfeito com o nome.
Algumas pessoas talvez confundam com uma falsificação da RaceFace vendida no AliExpress...

## Controle cibernético

Esta é ideias central por trás de qualquer tentativa de fazer algo funcionar. Tem muitos nomes: OKRs, Lean, TPS.
A ideia básicament: medir continuamente a saída de um sistema, compará-la com um objetivo desejado e fazer ajustes corretivos.

Ou, na versão clássica do LinkedIn:
> "Você não pode gerenciar aquilo que não consegue medir."

O próximo passo natural era conectar tudo ao Google Analytics.
Isso representou uma barreira mental para mim.
Nunca me enxerguei como alguém de marketing ou negócios.
O simples ato de instalar o GA neste site representou uma mudança de mentalidade.

### Cyberplumbing

Um bom design pode ser vítima de uma execução ruim.
A Arquitetura Medallion parece se encaixar muito bem no problema em teoria.
Na prática, as fronteiras são nebulosas: onde termina o Bronze? Onde começa o Silver? O que exatamente pertence a cada camada?
Especialmente quando se começa *ex nihilo*, sem as fronteiras fornecidas por um framework de lakehouse, você acaba se tornando o demiurgo imperfeito desse software — e inevitavelmente transmite algumas vibrações ruins para a base de código.

O Bronze foi fácil: pegar o arquivo.
Mas quem faz o parsing? O Bronze? Uma camada Raw + Bronze?
E no Silver? Quais são as regras quando algo dá errado? O que deve ser reprocessado?
Como lidar com registros duplicados?
Não é desperdício reprocessar tudo?

Passei bastante tempo tentando garantir que essas decisões não me causassem arrependimentos no futuro.

## E agora?

Agora que tenho o mínimo necessário funcionando (domínio + Google Analytics) e o scraper já cobre as principais fontes, as coisas começam a mudar.
Até aqui, todo problema tinha uma solução técnica. Os próximos problemas são centrados em pessoas.
Há pouco que eu possa fazer apenas como desenvolvedor. Agora estou usando o chapéu de SEO — ou talvez o de Marketing Engineer.
O foco passa a ser distribuição - Como colocar esse bebê na frente dos usuários?
Para ser sincero, é a minha primeira vez fazendo isso.
Percebo barreiras mentais pela frente. Vamos aprender conforme avançamos.
