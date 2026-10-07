# logpipe: roadmap de desenvolvimento

Oct 7, 2026 · @Francisco

## Ideia principal

O logpipe é um processador de logs local para Linux, em Go e no estilo pipeline: lê os logs do próprio computador, entende o que aconteceu e avisa. O nome é provisório.

**O problema.** O Ubuntu registra tudo que importa (logins, sudo, serviços que caem, falta de memória, erros de disco, pacotes instalados), mas ninguém lê o `journalctl` no dia a dia. Os sinais existem e passam despercebidos.

**A solução.** Um binário único, rodando como serviço do usuário, com dois modos sobre os mesmos filtros:

- **Vigiar:** acompanha o journal em tempo real e dispara uma notificação no desktop quando algo importante acontece.
- **Resumo:** uma vez por dia, processa o journal do dia anterior e gera um relatório em HTML e Markdown do que aconteceu na máquina.

**Fontes de dados:** o journald é a principal, porque recebe sshd, sudo, kernel e todos os serviços do systemd. `/var/log/dpkg.log` e `/var/log/apt/history.log` cobrem pacotes; `auth.log` e `kern.log` entram quando o rsyslog estiver instalado.

**O que ele demonstra no portfólio:**

- Arquitetura pipeline com pontas intercambiáveis: o mesmo miolo serve ao modo contínuo e ao modo lote.
- Concorrência em Go: goroutines, channels, backpressure e shutdown sem perda.
- Integração com o sistema operacional: subprocesso do journalctl, cursores, inode, systemd e notificações do desktop.
- Regras com estado: janelas de tempo, limiares e cooldown testados com relógio falso.
- Rigor de medição: consumo em repouso e throughput documentados.

**Fora do escopo:**

- Enviar logs para fora da máquina: tudo fica local.
- Monitorar vários computadores.
- Substituir ferramentas de segurança como fail2ban ou auditd: o logpipe avisa, não bloqueia.
- Interface gráfica além da notificação e do relatório.

## Princípios de design

Oito regras valem para todas as fases; quando uma decisão futura conflitar com elas, a decisão vira ADR.

1. **Transformers e testers são funções puras.** Recebem um evento e devolvem um evento, um descarte ou um erro. Não conhecem channels, goroutines, config, relógio nem métricas.
2. **Estado mora nas pontas.** Só producers (posição de leitura) e consumers (janelas de alerta, agregados do dia) guardam estado. Cada um roda numa goroutine só, então dispensa locks.
3. **O runner é dono dos pipes.** Só o package `pipeline` cria channels, sobe goroutines e conduz o shutdown em cascata.
4. **A ordem vem da configuração.** Reordenar, incluir ou remover filtros e regras é editar o YAML, sem recompilar.
5. **Passar o ponteiro transfere a posse.** Quem envia um `*Event` pelo channel não toca mais nele.
6. **Nada se perde em silêncio.** Falha de parse vira tag, erro vai para a DLQ, descarte de tester é contado.
7. **Checkpoint só depois do processamento.** O cursor do journal só avança quando o consumer termina o evento.
8. **Leve e sem root.** Roda o dia todo como usuário do grupo `adm`, sem portas abertas, com a biblioteca padrão mais uma biblioteca YAML.

## Arquitetura

O logpipe monta dois pipelines a partir da mesma config: o miolo (parse, drop\_if, classify) é igual e só as pontas mudam. No modo vigiar, o producer segue o journal e o consumer dispara alertas; no modo resumo, o producer lê um dia inteiro, termina sozinho, e o consumer agrega e escreve o relatório.

&#91;embedded content: dois modos · mesmo miolo, producers e consumers diferentes\]

O tracejado verde é a confirmação: o cursor do journal só vira checkpoint depois que o consumer de alertas processou o evento. O modo resumo não precisa de checkpoint, porque o período define o que ler.

| Papel no estilo pipeline | Interface Go | Implementações (fase) |
| --- | --- | --- |
| Producer: origem dos dados | `Producer` | stdin (1); journald contínuo e por período, tail de arquivo (3) |
| Transformer: altera o evento | `Transformer` | parser journald (1); syslog, dpkg, regex, add\_fields, rename, timestamp, classify (2) |
| Tester: decide se o evento segue | `Tester` | priority (1); keep\_if, drop\_if (2) |
| Consumer: destino final | `Consumer` | stdout (1); alert (4); summary (5) |

**Contratos.** O tipo do evento e o callback de confirmação ficam em `event`, que não importa nenhum outro package interno:

```go
package event

type Event struct {
    Time     time.Time
    Priority Priority // 0 (emerg) a 7 (debug), como no syslog
    Message  string
    Unit     string   // unidade do systemd ou identificador do processo
    Kind     string   // preenchido pelo classify: "ssh_login_failed", "oom_kill"...
    Fields   map[string]any
    Tags     []string
    Raw      string

    Source   string // "journald" ou caminho do arquivo
    Position string // cursor do journal ou inode:offset, usado no checkpoint
}

// AckFunc confirma que o consumer terminou de processar o evento.
type AckFunc func(e *Event)
```

As interfaces ficam em `pipeline`, onde são consumidas. Os filtros as implementam sem importar `pipeline`:

```go
package pipeline

type Producer interface {
    Run(ctx context.Context, out chan<- *event.Event) error
}

type Transformer interface {
    Name() string
    Transform(e *event.Event) (*event.Event, error)
}

type Tester interface {
    Name() string
    Keep(e *event.Event) bool
}

type Consumer interface {
    Name() string
    Run(in <-chan *event.Event, ack event.AckFunc) error
}
```

- O producer não fecha `out`: o runner fecha quando `Run` retorna. No modo resumo, `Run` retorna sozinho ao fim do período; no modo vigiar, só com o cancelamento do contexto.
- O consumer só retorna quando `in` fecha e o trabalho final termina. No resumo, é nesse momento que o relatório é escrito.
- `Name()` identifica o stage nas métricas e na DLQ.
- Internamente, Transformer e Tester viram o mesmo `stageFunc`, em que retornar `nil` significa "descartado".

## Módulo Go e packages

O projeto é um único módulo Go, `github.com/FranciscoHonorat/logpipe`, com todo o código em `internal/`. É um binário só e não há API pública antes da v1.0, então separar em vários módulos só traria custo de versionamento. Requer Go 1.22 ou mais recente e Linux com systemd.

Layout ao final da v1.0:

```text
logpipe/
├── go.mod
├── Makefile
├── cmd/logpipe/          binário: vigiar, resumo, validate, test, status, install, doctor
├── internal/
│   ├── event/            Event, Priority, tags, AckFunc
│   ├── pipeline/         contratos, runner, adaptadores, shutdown
│   ├── config/           structs, leitura do YAML, validação
│   ├── paths/            diretórios XDG de config, estado e relatórios
│   ├── registry/         nome no YAML para construtor do filtro
│   ├── producer/         stdin, journald, file
│   ├── journal/          journalctl como subprocesso: follow, período, cursor
│   ├── tail/             leitura contínua com rotação e arquivos .gz
│   ├── checkpoint/       posições confirmadas, persistência atômica
│   ├── parser/           journald, syslog, dpkg, regex
│   ├── transformer/      add_fields, rename, timestamp
│   ├── classify/         motor de classificação e regras do Ubuntu embutidas
│   ├── tester/           priority, keep_if, drop_if
│   ├── consumer/         stdout, alert, summary
│   ├── rules/            janelas, limiares e cooldown dos alertas
│   ├── notify/           notificação no desktop
│   ├── alertlog/         histórico de alertas disparados por dia
│   ├── report/           agregação do dia, HTML e Markdown
│   ├── backoff/          retry exponencial com jitter
│   ├── dlq/              eventos com erro em JSON Lines
│   ├── telemetry/        métricas do próprio logpipe
│   ├── install/          units do systemd de usuário
│   └── testutil/         fakes de producer, consumer, relógio e notificador
├── testdata/             amostras reais anonimizadas e golden files
├── examples/             config comentada, units do systemd
├── scripts/              simulate.sh, bench.sh
└── docs/
    ├── adr/
    └── PERFORMANCE.md
```

| Package | Responsabilidade | Nasce na fase | Importa (internos) |
| --- | --- | --- | --- |
| `event` | Event, Priority, tags, AckFunc | 1 | nenhum |
| `pipeline` | contratos, runner, adaptadores, shutdown | 1 | event (telemetry na 6) |
| `producer` | stdin (1); journald e file (3) | 1 | event, journal, tail, checkpoint |
| `parser` | journald (1); syslog, dpkg, regex (2) | 1 | event |
| `tester` | priority (1); keep\_if, drop\_if (2) | 1 | event |
| `consumer` | stdout (1), alert (4), summary (5) | 1 | event, rules, notify, alertlog, report |
| `testutil` | fakes; relógio e notificador falsos (4) | 1 | event |
| `config` | YAML e validação | 2 | nenhum |
| `paths` | diretórios XDG | 2 | nenhum |
| `registry` | monta os dois pipelines a partir da config | 2 | config, pipeline, todos os filtros |
| `transformer` | add\_fields, rename, timestamp | 2 | event |
| `classify` | motor de classificação, regras do Ubuntu via `go:embed` | 2 | event |
| `journal` | subprocesso do journalctl, cursor, reinício | 3 | backoff |
| `tail` | leitura contínua, rotação, arquivos .gz | 3 | nenhum |
| `checkpoint` | rastreia e persiste posições confirmadas | 3 | event |
| `backoff` | retry exponencial com jitter | 3 | nenhum |
| `dlq` | eventos com erro em JSON Lines | 3 | event |
| `rules` | janelas, limiares, cooldown, tipo `Alert` | 4 | event |
| `notify` | notificação no desktop | 4 | nenhum |
| `alertlog` | histórico de alertas por dia | 4 | rules |
| `report` | agregação do dia, HTML e Markdown | 5 | event, alertlog |
| `telemetry` | contadores e latência por stage, snapshot de status | 6 | nenhum |
| `install` | units do systemd de usuário | 7 | paths |

**Regras de dependência**, das camadas de baixo para cima:

1. **Base:** `event`, `config`, `paths`, `backoff`, `tail`, `notify`, `telemetry`. Não importam nenhum package interno, exceto `event`.
2. **Apoio:** `journal`, `checkpoint`, `dlq`, `rules`, `alertlog`, `report`, `install`. Importam só a base e uns aos outros sem ciclo.
3. **Filtros:** `producer`, `parser`, `transformer`, `classify`, `tester`, `consumer`. Importam base e apoio; nenhum filtro importa `pipeline` nem outro filtro.
4. **Orquestração:** `pipeline` conhece apenas as interfaces, `event` e `telemetry`.
5. **Montagem:** `registry` é o único package que conhece config, pipeline e todos os filtros.
6. **Entrada:** `cmd/logpipe` importa `config`, `paths`, `registry`, `pipeline` e `install`.

Um teste no CI pode garantir essas regras: ele roda `go list -deps` em cada filtro e falha se aparecer `internal/pipeline`.

## Fases em uma olhada

Oito fases, cada uma fechada por um gate que se verifica com teste ou demo, nunca por impressão.

&#91;embedded content: roadmap · 8 fases, um gate em cada\]

A Fase 1 já roda contra o journal de verdade via pipe. As Fases 3 e 4 são as maiores, porque é nelas que o logpipe passa a conversar com o sistema operacional.

## Fase 0: Fundação

Repositório pronto para receber código, com CI rodando testes com race detector desde o primeiro commit. Tamanho: P. Nenhum package de produção ainda.

- [ x] `go mod init github.com/FranciscoHonorat/logpipe`
- [x ] Pastas vazias: `cmd/`, `internal/`, `testdata/`, `examples/`, `scripts/`, `docs/adr/`
- [ x] Makefile com `build`, `test` (`go test -race ./...`), `lint`, `bench`
- [ x] Lint com `go vet` e staticcheck
- [ ] GitHub Actions rodando `make lint test` em cada push e pull request
- [ ] Conferir o ambiente: journal persistente (`/var/log/journal` existe) e seu usuário no grupo `adm`
- [ ] Amostras reais em `testdata/`: `journalctl -o json --since today`, `dpkg.log`, `apt/history.log`, e `auth.log` e `kern.log` se existirem
- [ ] Anonimizar as amostras antes do commit: hostname, usuário, IPs
- [ ] README com a ideia principal e o diagrama dos dois modos
- [ ] ADR 0001: estilo pipeline com filtros puros e estado nas pontas

**Pronto quando:** o CI fica verde num commit com um teste trivial.

## Fase 1 (v0.1): Esqueleto do pipeline

`journalctl -o json -f | logpipe --priority=warning` mostra em tempo real, legível, só os eventos de warning para cima, e Ctrl+C sai sem perder o que estava em trânsito. Tamanho: M. Fluxo: `stdin → parser journald → tester priority → stdout`.

| Package | Arquivos | O que entra |
| --- | --- | --- |
| `event` | `event.go`, `priority.go`, `tags.go` | `Event`, `New(raw, source, position)`; `Priority` de 0 (emerg) a 7 (debug) com `ParsePriority` aceitando `warning`, `warn`, `err`, `4`; `AddTag`, `HasTag`; `AckFunc` |
| `pipeline` | `stage.go`, `adapt.go`, `runner.go`, `errors.go` | as quatro interfaces; `stageFunc` e adaptadores de Transformer e Tester; `Pipeline.Run(ctx)`; `ErrorHandler`, que nesta fase só loga em stderr |
| `producer` | `stdin.go` | `Stdin` com `bufio.Reader` e limite de linha configurável |
| `parser` | `journald.go` | decodifica a linha JSON do journal: `__REALTIME_TIMESTAMP` (microssegundos), `PRIORITY`, `MESSAGE`, `_SYSTEMD_UNIT` ou `SYSLOG_IDENTIFIER`, `__CURSOR`; o resto vai para `Fields`; falha vira a tag `_parse_failure` |
| `tester` | `priority.go` | `MaxPriority`: passa se a prioridade for igual ou mais grave que o limite |
| `consumer` | `stdout.go` | modo texto (hora, unit, mensagem, cor por prioridade) e modo JSON Lines; chama `ack` após cada escrita |
| `testutil` | `fakes.go` | `SliceProducer`, `CollectConsumer`, `FailingTransformer` |
| `cmd/logpipe` | `main.go` | flags, `signal.NotifyContext`, pipeline montado à mão |

**Runner:**

- Cada stage roda numa goroutine, ligada à seguinte por um channel com buffer (256 por padrão).
- O runner fecha o channel do producer quando `Run` retorna; o resto fecha em cascata.
- `Run` espera todos terminarem e devolve o primeiro erro fatal.
- Timeout de shutdown (padrão 10 s): se o consumer travar, o processo sai mesmo assim.
- `Consumer` já recebe `AckFunc`, no-op nesta fase, para o contrato não quebrar na Fase 3.

**Testes:**

- Table-driven no parser journald, usando as amostras de `testdata/`.
- Runner com fakes: N eventos entram, N saem, na mesma ordem.
- Shutdown: cancelar o contexto no meio; todo evento lido foi consumido ou contado como descartado.
- Erro num transformer chega ao `ErrorHandler` com o nome do stage.
- Tudo com `-race`.

**Armadilhas:**

- No JSON do journalctl, `MESSAGE` pode vir como array de números (conteúdo binário ou não UTF-8), e um campo repetido vira array de strings. O parser precisa aceitar os três formatos.
- `PRIORITY` e os timestamps chegam como string, não como número.
- `bufio.Scanner` tem limite padrão de 64 KB por linha e para com `ErrTooLong`; prefira `bufio.Reader`.
- Ler `os.Stdin` não é cancelável por contexto: leia numa goroutine própria e faça `select` com `ctx.Done()`.

**Pronto quando:**

- [ ] O comando de exemplo funciona com o journal real e com as amostras de `testdata/`
- [ ] Ctrl+C no meio do fluxo não perde eventos já lidos
- [ ] `go test -race ./...` verde, com parser e tester acima de 80% de cobertura
- [ ] ADR 0002: shutdown em cascata e posse do evento

**Conceitos praticados:** goroutines, channels com buffer, posse de dados, `context`, sinais do sistema operacional, formato do journald.

## Fase 2 (v0.2): Configuração e classificação

O pipeline passa a ser montado a partir de um YAML, e o `classify` dá nome aos eventos que importam: login falho, sudo, OOM, serviço que caiu. Tamanho: M. Entregáveis: `journalctl -o json --since today | logpipe test` mostrando o que cada stage fez com cada evento, e `logpipe validate`.

| Package | Arquivos | O que entra |
| --- | --- | --- |
| `config` | `config.go`, `load.go`, `validate.go` | structs dos modos (`vigiar`, `resumo`), filtros, regras e saídas; erros com caminho, como `classify.rules[3].pattern: regex inválida` |
| `paths` | `paths.go` | `XDG_CONFIG_HOME`, `XDG_STATE_HOME`, `XDG_DATA_HOME`, com fallback para `~/.config`, `~/.local/state` e `~/.local/share` |
| `registry` | `registry.go`, `builtin.go` | `map[string]Factory`; `Build(cfg, modo) (*pipeline.Pipeline, error)`; `builtin.go` registra todos os filtros |
| `parser` | `syslog.go`, `dpkg.go`, `regex.go` | syslog no formato `Oct  7 15:48:01 host sshd[1234]: msg`; dpkg.log com ação (install, upgrade, remove), pacote e versões; regex genérico com grupos nomeados |
| `transformer` | `fields.go`, `timestamp.go` | `add_fields`, `rename`, `remove_fields`; `timestamp` com lista de layouts |
| `classify` | `classify.go`, `rule.go`, `ubuntu.yaml` | regras com `match` (unit, identificador, prioridade) e `pattern` regex cujos grupos nomeados viram campos; a primeira regra que casa define `Kind`; regras do Ubuntu embutidas com `go:embed` e sobrescrevíveis pela config do usuário |
| `tester` | `match.go` | `keep_if` e `drop_if` com `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `contains`, `regex`, `exists`, `in` |
| `cmd/logpipe` | `run.go`, `validate.go`, `test.go` | subcomandos; `test` mostra o evento depois de cada stage |

**Catálogo inicial de tipos (`Kind`):**

| Kind | Origem | Campos extraídos |
| --- | --- | --- |
| `ssh_login_failed` | sshd: senha errada ou usuário inválido | usuário, IP |
| `ssh_login_ok` | sshd: login aceito | usuário, IP, método |
| `sudo` | sudo: comando executado ou senha errada | usuário, comando |
| `oom_kill` | kernel: processo morto por falta de memória | processo, PID |
| `disk_error` | kernel: erro de I/O em dispositivo | dispositivo |
| `unit_failed` | systemd: unidade terminou em falha | unit |
| `segfault` | kernel: falha de segmentação | processo |
| `usb_device` | kernel: dispositivo USB conectado | fabricante, produto |
| `package_change` | dpkg: install, upgrade, remove | pacote, versão |
| `boot` | journald: novo `_BOOT_ID` | id do boot |

As mensagens exatas variam entre versões do Ubuntu; as regex nascem das amostras reais em `testdata/`, nunca de memória.

Config como ela fica ao final da Fase 5 (os tipos `journald`, `alert` e `summary` já são validados agora, mas só funcionam nas Fases 3, 4 e 5):

```yaml
filters:
  - parse: journald
  - drop_if: { field: priority, gt: 5 }
  - drop_if: { field: unit, in: [unidade-barulhenta.service] }
  - classify: { rules: builtin:ubuntu }

classify_rules:
  - kind: docker_restart
    match: { unit: docker.service }
    pattern: 'Starting Docker Application Container Engine'

modes:
  vigiar:
    input: { type: journald, follow: true }
    output: { type: alert }
  resumo:
    input: { type: journald, period: day, extra: [/var/log/dpkg.log] }
    output: { type: summary }
```

**Testes:**

- Golden files: journal anonimizado em `testdata/`, eventos classificados esperados em `testdata/golden/`, flag `-update` para regenerar.
- Cada regra embutida tem ao menos um exemplo positivo e um negativo tirados das amostras.
- Fuzz (`go test -fuzz`) nos parsers syslog e dpkg: nenhuma entrada pode causar panic.
- Validação de config: um caso por tipo de erro.

**Armadilhas:**

- Compilar regex por evento destrói o desempenho; compile no construtor do filtro.
- O regex do Go (RE2) não tem backtracking catastrófico, mas também não aceita lookahead; escreva os padrões sem ele.
- Primeira regra que casa vence: regras específicas antes das genéricas.
- A data do syslog não tem ano e usa espaço antes de dia com um dígito; em janeiro, linhas de dezembro são do ano anterior.

**Pronto quando:**

- [ ] Amostras de journal, dpkg.log e auth.log classificadas com golden files
- [ ] `validate` aponta o erro com o caminho exato
- [ ] Fuzz roda 1 minuto sem falha em cada parser
- [ ] ADR 0003: formato da config e registry
- [ ] ADR 0004: modelo de classificação e regras embutidas

**Conceitos praticados:** parsing, regex RE2, `go:embed`, fuzzing, golden files, injeção de dependência via registry.

## Fase 3 (v0.3): Fontes do sistema e checkpoint

O logpipe deixa de depender de pipe e passa a ler o sistema sozinho: journald como subprocesso, arquivos de log com rotação e um checkpoint que permite reiniciar sem perder eventos. Tamanho: G. Entregáveis: `logpipe vigiar --output stdout` seguindo o journal, e o modo lote com `--since` e `--until` lendo um período e terminando sozinho.

| Package | Arquivos | O que entra |
| --- | --- | --- |
| `journal` | `journal.go`, `cursor.go` | inicia `journalctl -o json` com `--follow --after-cursor=X` (contínuo) ou `--since`/`--until` (período) via `exec.CommandContext`; lê stdout linha a linha e captura stderr; se o journalctl morrer, reinicia com backoff a partir do último cursor |
| `producer` | `journald.go`, `file.go` | journald usa `journal`, file usa `tail`; os dois preenchem `Source` e `Position` |
| `tail` | `tail.go`, `fileid_unix.go`, `gzip.go` | polling (250 ms); rotação por troca de inode e truncamento; linha incompleta guardada até o `\n`; no modo lote, lê também as versões rotacionadas (`.1`, `.gz`) com `compress/gzip` |
| `checkpoint` | `tracker.go`, `store.go` | última posição confirmada por fonte (cursor do journal ou inode:offset); escrita atômica (arquivo temporário, `fsync`, `rename`) em `~/.local/state/logpipe/`; flush a cada 1 s e no shutdown |
| `backoff` | `backoff.go` | exponencial com full jitter, teto de espera, respeita `ctx` |
| `dlq` | `writer.go` | JSON Lines em `~/.local/state/logpipe/dlq.jsonl` com evento original, stage e motivo; rotação por tamanho |
| `pipeline` | `runner.go` | liga a `AckFunc` ao `Tracker`; o `ErrorHandler` passa a gravar na DLQ |
| `event` | `event.go` | `Position` passa a ser preenchido |

**Por que o checkpoint funciona:**

- O cursor do journal é estável e opaco: `--after-cursor` retoma exatamente do próximo evento, inclusive depois de um reboot.
- Cada stage é uma goroutine só, então a ordem por fonte se mantém até o consumer, e a última posição confirmada é uma marca d'água válida.
- Eventos descartados por tester não são confirmados; depois de um reinício, o trecho é relido e descartado de novo, sem efeito visível.
- Como o flush é a cada 1 s, um `kill -9` pode repetir até 1 s de eventos. Perder, nunca.

**Testes:**

- `journal` com um script falso no lugar do journalctl (caminho configurável) que emite JSON e morre no meio: o producer reinicia do cursor certo.
- `tail` com `t.TempDir()`: rename + create, copytruncate, linha escrita em duas partes, leitura de `.gz`.
- `checkpoint`: escrita interrompida não corrompe o arquivo anterior.
- Teste real em `scripts/`: `logger -t logpipe-teste "evento N"` gerando eventos numerados, com `kill -9` e reinício no meio, conferindo que todos os números aparecem.

**Armadilhas:**

- Sem o grupo `adm` (ou `systemd-journal`), o journalctl mostra só os eventos do próprio usuário e deixa apenas uma dica no stderr. O logpipe precisa checar os grupos na inicialização e avisar claramente.
- `exec.CommandContext` mata o processo, mas o pipe de stdout precisa ser drenado e o `Wait` chamado, senão sobra processo zumbi.
- Usar o caminho como chave do checkpoint de arquivo: depois do rename, ele aponta para outro arquivo.
- Avançar o checkpoint na leitura em vez de no processamento transforma a garantia em at-most-once.

**Pronto quando:**

- [ ] 20 `kill -9` seguidos com eventos do `logger` chegando: nenhum perdido
- [ ] journalctl morto à força é reiniciado sozinho do cursor certo
- [ ] Rotação do dpkg.log simulada sem perda
- [ ] ADR 0005: journalctl como subprocesso em vez de biblioteca com cgo, e semântica do checkpoint

**Conceitos praticados:** subprocessos e pipes, cursores, inode, escrita atômica, `fsync`, semânticas de entrega, permissões no Linux.

## Fase 4 (v0.4): Alertas

O modo vigiar ganha o consumer de alertas: regras com janela de tempo decidem o que merece aviso, e o aviso chega como notificação no desktop, sem spam. Tamanho: G. Entregável: uma força bruta simulada no SSH gera uma notificação, não seis.

| Package | Arquivos | O que entra |
| --- | --- | --- |
| `rules` | `rule.go`, `window.go`, `engine.go` | tipos de regra: `any` (todo evento do Kind alerta), `threshold` (N eventos da mesma chave numa janela) e `new` (primeira vez que um valor aparece, com os valores conhecidos persistidos); cooldown por regra e chave; `Engine.Feed(e, now) []Alert`, puro, com relógio injetado |
| `notify` | `notifier.go`, `notifysend.go`, `log.go` | interface `Notifier`; implementação com `notify-send` (título, corpo, urgência, ícone); fallback que só registra em log quando não há sessão gráfica |
| `alertlog` | `alertlog.go` | grava cada alerta em `~/.local/state/logpipe/alertas-AAAA-MM-DD.jsonl`; leitura por dia para o resumo |
| `consumer` | `alert.go` | alimenta o `Engine`, envia os alertas ao `Notifier` e ao `alertlog`, depois chama `ack`; na inicialização, reconstrói os cooldowns a partir do alertlog do dia; eventos mais velhos que `max_age` (ex.: 10 min) entram no histórico mas não notificam |
| `testutil` | `clock.go`, `notifier.go` | relógio falso controlável e notificador que só coleta |

Regras de exemplo:

```yaml
alerts:
  - name: forca-bruta-ssh
    kind: ssh_login_failed
    type: threshold
    key: ip
    count: 5
    window: 2m
    cooldown: 30m
    urgency: critical
  - name: memoria-esgotada
    kind: oom_kill
    type: any
    cooldown: 5m
  - name: servico-caiu
    kind: unit_failed
    type: any
    key: unit
    cooldown: 15m
  - name: usb-desconhecido
    kind: usb_device
    type: new
    key: product
```

**Como simular sem risco** (vira `scripts/simulate.sh`):

1. Força bruta: `logger -t sshd -p auth.warning "Failed password for invalid user teste from 203.0.113.7 port 22 ssh2"` seis vezes seguidas (203.0.113.0/24 é faixa reservada para documentação).
2. Serviço caindo: `systemd-run --user --unit=logpipe-teste false` cria uma unidade que falha na hora.
3. OOM controlado: `systemd-run --user -p MemoryMax=50M` rodando um programa que aloca memória sem parar; o kernel mata só aquele processo. Funciona quando o systemd delega o controle de memória ao usuário; senão, use uma VM.

**Testes:**

- `Engine` com relógio falso: 4 falhas em 2 min não alertam, a 5ª alerta, a 6ª cai no cooldown, e a janela desliza corretamente.
- Consumer com notificador falso: uma sequência de eventos gera exatamente os alertas esperados.
- `scripts/simulate.sh` contra o logpipe rodando de verdade.

**Armadilhas:**

- A notificação só aparece se o processo enxergar a sessão gráfica (`DBUS_SESSION_BUS_ADDRESS`). Por isso o logpipe roda como serviço do usuário, não do sistema.
- Campos com `_` na frente (`_COMM`, `_SYSTEMD_UNIT`) são preenchidos pelo journald e não podem ser forjados; `SYSLOG_IDENTIFIER` pode, via `logger`. Regras de segurança casam pelos campos confiáveis; os testes com `logger` usam uma flag `--trust-identifier`.
- Depois de um reinício, o journal entrega eventos antigos de uma vez: sem `max_age`, sai uma rajada de alertas velhos.
- Janela que guarda eventos inteiros cresce sem limite: guarde só timestamps por chave e limpe as chaves expiradas.
- Mensagem do OOM por cgroup é diferente da do OOM global; a regra `oom_kill` precisa casar as duas.

**Pronto quando:**

- [ ] Força bruta simulada: 1 notificação, não 6
- [ ] Serviço que falha e OOM controlado notificam em menos de 2 s
- [ ] Reiniciar o logpipe não repete alertas já disparados
- [ ] ADR 0006: modelo de regras com janela, cooldown e campos confiáveis

**Conceitos praticados:** máquinas de estado, janelas deslizantes, relógio injetável, integração com o desktop Linux, confiança em dados de entrada.

## Fase 5 (v0.5): Resumo diário

O modo resumo processa o journal de um dia inteiro com os mesmos filtros e gera um relatório do que aconteceu na máquina. Tamanho: M. Entregável: `logpipe resumo --dia ontem` gera HTML e Markdown em segundos, e um timer do systemd roda isso sozinho todo dia.

| Package ou pasta | Arquivos | O que entra |
| --- | --- | --- |
| `producer` | `journald.go` | modo período: `--since` e `--until` do dia no fuso local; termina sozinho ao fim do período, o que fecha o pipeline em cascata |
| `report` | `model.go`, `aggregate.go`, `render.go`, `templates/` | `DaySummary` com tempo ligado por boot, alertas do dia (lidos do `alertlog`), segurança (logins aceitos e falhos, IPs mais frequentes, sudo), sistema (units com falha, OOM, erros de disco, segfaults), pacotes e as 5 maiores fontes de erro; templates embutidos com `go:embed`, HTML via `html/template` |
| `consumer` | `summary.go` | alimenta o agregador; quando o channel fecha, renderiza e grava em `~/.local/share/logpipe/resumos/AAAA-MM-DD.html` e `.md`, e notifica "Resumo de ontem pronto" |
| `cmd/logpipe` | `resumo.go` | `--dia ontem`, `hoje` ou `AAAA-MM-DD`; `--formato html,md`; `--abrir` (via `xdg-open`) |
| `examples/systemd` | `logpipe-resumo.service`, `logpipe-resumo.timer` | `OnCalendar` diário às 08:00 e `Persistent=true`, para rodar no próximo boot se o PC estava desligado no horário |

**Tempo ligado:** cada `_BOOT_ID` do dia vira um intervalo do primeiro ao último evento daquele boot. Limitação aceita na v1: suspensão conta como ligado.

Esboço do relatório em Markdown (números ilustrativos):

```markdown
# Resumo de 2026-10-06

Ligado das 07:42 às 23:10 (1 boot) · 3 alertas · 1 serviço com falha

## Segurança
- 2 logins SSH aceitos, 14 falhas (IP mais frequente: 203.0.113.7)
- 5 comandos com sudo

## Sistema
- docker.service falhou às 14:03
- Nenhum OOM, nenhum erro de disco

## Pacotes
- 12 atualizados, 1 instalado (htop)

## Maiores fontes de erro
1. gnome-shell: 41
2. NetworkManager: 9
```

**Testes:**

- Golden: journal anonimizado de um dia gera o `DaySummary` esperado.
- Fuso: o dia é delimitado no horário local, não em UTC; teste com eventos às 22h e à 01h.
- Dia sem nenhum boot gera um relatório "computador desligado", sem erro.

**Armadilhas:**

- Delimitar o dia em UTC corta 3 horas do dia em Fortaleza.
- O timer pode rodar antes de a sessão gráfica existir: a notificação falha, mas o arquivo precisa ser gravado mesmo assim.
- O journal tem limite de tamanho e apaga dias antigos. Avise quando o período pedido for anterior ao primeiro evento disponível.

**Pronto quando:**

- [ ] Resumo de ontem gerado em menos de 10 s num dia típico
- [ ] Timer roda no boot seguinte quando o PC estava desligado às 08:00
- [ ] ADR 0007: o mesmo pipeline em modo contínuo e em lote

**Conceitos praticados:** processamento em lote, agregação, fusos horários, templates, timers do systemd.

## Fase 6 (v0.6): Leveza e observabilidade

Um serviço que roda o dia inteiro no notebook precisa provar que é leve: o logpipe passa a medir a si mesmo e ganha números de desempenho documentados. Tamanho: M. Entregável: `docs/PERFORMANCE.md` com consumo em repouso e throughput do modo lote.

| Package ou pasta | Arquivos | O que entra |
| --- | --- | --- |
| `telemetry` | `metrics.go`, `status.go` | por stage: eventos de entrada, saída, descartados e erros, latência, ocupação do channel (`len/cap`); snapshot gravado em `~/.local/state/logpipe/status.json` a cada 10 s, sem abrir porta |
| `pipeline` | `instrument.go` | decorator que envolve cada stage com telemetria; os filtros continuam sem saber de métricas |
| `cmd/logpipe` | `status.go` | `logpipe status`: modo ativo, eventos por minuto, alertas de hoje, últimos erros, CPU e memória do processo |
| `scripts` | `bench.sh` | journal sintético grande passando pelo modo lote, com os números coletados |

Metas sugeridas, a confirmar medindo: em repouso, menos de 1% de CPU e menos de 30 MB de memória residente; no modo lote, um dia típico em poucos segundos.

Como ler as métricas: o stage cujo channel de entrada vive cheio é o gargalo. Os anteriores ficam bloqueados esperando por ele.

**Experimentos, um por vez, medindo antes e depois:**

1. `go test -bench -benchmem` em cada filtro: ns/op e alocações por evento.
2. pprof de CPU e memória no modo lote sobre uma semana de journal.
3. Decodificar o JSON do journal numa struct com só os campos usados, em vez de `map[string]any`.
4. Lotes (`[]*Event`) nos channels em vez de eventos soltos. Muda o contrato, então pede ADR.
5. `sync.Pool` para `Event` e buffers.
6. Em repouso: confirmar que nenhum ticker acorda a CPU sem necessidade (polling do tail, flush do checkpoint).

**Pronto quando:**

- [ ] `logpipe status` mostra os números ao vivo
- [ ] `PERFORMANCE.md` com hardware, cenários, números e gargalo
- [ ] Metas de repouso atingidas ou o desvio justificado
- [ ] ADR 0008: otimizações mantidas e descartadas, com os números

**Conceitos praticados:** profiling, alocação em Go, custo de wakeups, filas e gargalos.

## Fase 7 (v1.0): Instalação e portfólio

Qualquer pessoa com Ubuntu instala o logpipe com um comando e passa a receber alertas e resumos sem configurar nada. Tamanho: M. Entregável: tag `v1.0.0` com binários e `logpipe install`.

| Package ou pasta | Arquivos | O que entra |
| --- | --- | --- |
| `install` | `install.go`, `units/` | gera `logpipe-vigiar.service`, `logpipe-resumo.service` e `logpipe-resumo.timer` em `~/.config/systemd/user`; roda `systemctl --user daemon-reload` e `enable --now`; `uninstall` desfaz tudo |
| `cmd/logpipe` | `install.go`, `doctor.go` | `install`, `uninstall`; `doctor` checa grupo `adm`, journal persistente, `notify-send` instalado e sessão gráfica, dizendo como corrigir cada item |
| `.github/workflows` | `release.yml` | binários linux amd64 e arm64 anexados à release |
| `examples` | `config.yaml` | config comentada com todas as regras padrão |
| `docs` | `README.md`, `adr/`, `PERFORMANCE.md` | documentação final |

**README, nesta ordem:** o problema em três linhas, GIF da notificação aparecendo, imagem de um resumo, instalação em um comando, regras padrão, como escrever uma regra própria com `logpipe test`, desempenho e links para as ADRs.

**Pronto quando:**

- [ ] Numa VM Ubuntu limpa: `install` e, em minutos, uma notificação de teste e um resumo
- [ ] `doctor` explica cada problema de ambiente com a correção
- [ ] O README explica o projeto para um recrutador em 30 segundos e para um dev em 5 minutos
- [ ] Post no LinkedIn sobre uma decisão técnica: o mesmo pipeline em dois modos, ou alertas sem spam

**Conceitos praticados:** systemd de usuário, distribuição de binários, experiência de instalação, documentação para públicos diferentes.

## Depois da v1.0

Ideias para quando o núcleo estiver estável, sem ordem fixa:

- Pacote `.deb` para instalar pelo apt.
- Outros canais de alerta, como Telegram ou e-mail, para quando você não está na frente do PC.
- Logs de apps próprias e de containers Docker, com junção de stack traces em várias linhas.
- Histórico consultável em SQLite, com busca pela linha de comando.
- Mascarar dados pessoais antes de compartilhar um resumo.
- Painel ao vivo no terminal (TUI).
- Suporte a outras distribuições, com caminhos e formatos diferentes.
