# Batcher

**Turn reusable AI instructions and structured source items into batches of
traceable requests, collected answers, and usable files.**

Batcher is the batch AI processing application in the
[FUDD ecosystem](https://github.com/whatsupfudd). Written in Haskell, it combines
Ginger templates, PostgreSQL workflow state, S3-compatible asset storage, and
an OpenAI Batch API adapter. Its command-line tools cover prompt preparation,
submission, result collection, and document or code extraction.

The project began as **docproc**, a file-based document-production utility.
**Batcher** is the project name; **`ai_batcher`** is this repository; the Haskell
package and executable are still named **`docproc`**.

> **Status:** active development, package version `0.1.0.0`. The CLI contains
> the implemented processing workflows. The HTTP API is scaffolding, and the
> automated test suite is a placeholder. Read the build requirements and
> operational limits before running a production workload.

## Why Batcher?

Repeated AI work becomes easier to manage when instructions, input data,
execution state, and outputs have explicit representations. Batcher provides:

- **Reusable prompts:** render a Ginger template once per source item, with
  item metadata and production information available to the template.
- **Recorded provenance:** associate each production with stored template and
  source assets; retain request text, events, and result metadata.
- **Asynchronous execution:** submit requests in batches, poll their provider
  status, and collect answers through separate worker engines.
- **Context reuse:** separate shared instruction blocks from item-specific
  messages and build provider cache fields from the configured model policy.
- **Recoverable records:** keep workflow state in PostgreSQL and raw provider
  output in S3-compatible storage.
- **Useful outputs:** export collected answers as text or DOCX, or extract
  code and HTML/JavaScript from responses following defined conventions.

Typical uses include generating related documents from a common brief,
processing a catalogue of text items, and generating source files against
shared software specifications. Within FUDD, Batcher supplies batch execution
and artifact handling that other AI-related applications can build upon.

## Contents

- [Build](#build)
- [Configure](#configure)
- [Run a first production](#run-a-first-production)
- [Templates and source data](#templates-and-source-data)
- [Commands](#commands)
- [Image-analysis experiment](#image-analysis-experiment)
- [Architecture](#architecture)
- [Operational limits](#operational-limits)
- [Development and contributions](#development-and-contributions)
- [License](#license)

## Build

The project uses Stack and Hpack. The checked-in [stack.yaml](stack.yaml)
selects **Stackage LTS 24.50** and sets `system-ghc: true`; provide a compatible
GHC installation. The `unix` dependency makes a Unix-like environment the
current build target.

```bash
git clone https://github.com/whatsupfudd/ai_batcher.git
cd ai_batcher
```

**Resolve the local dependency before building.** `stack.yaml` references
`../../../../Haskell/Minio/minio-hs`, which is outside this repository. Point
that entry at the compatible checkout used by your development environment.
The repository does not pin that checkout's revision; substituting a released
package requires compatibility testing.

```bash
stack build
stack exec docproc -- --help
```

Runtime requirements depend on the command:

| Workflow | Requirements |
| --- | --- |
| `producer`, `receiver`, and template storage | PostgreSQL; an existing S3-compatible bucket; OpenAI credentials for provider operations |
| `process` | PostgreSQL and an S3 configuration; the current command checks for S3 even though result conversion reads PostgreSQL |
| Legacy `load` and `fetch` | OpenAI credentials and local files |
| `extract`, `postp`, `printtest` | Local files |
| `gendoc` | Local HTML; a TeX installation with `xelatex`, `kpsewhich`, and suitable fonts for PDF output |
| `imgtest` | OpenAI credentials, a local image, and the bundled context file |

The executable loads a YAML configuration before dispatching commands,
including local conversion commands. Parser-generated `--help` can be used
without one.

## Configure

Configuration-file selection, in precedence order:

1. `--config PATH` or `-c PATH`.
2. The `docprocCONF` environment variable, with that exact capitalization.
3. `~/.fudd/docproc/config.yaml`.

The help text's `~/.docproc/config.yaml` path is outdated. Configuration files
must currently use the `.yaml` extension.

Create a configuration outside the repository, for example
`~/.fudd/docproc/config.yaml`:

```yaml
provider: openai

db:
  host: "127.0.0.1"
  port: 5432
  user: "batcher"
  passwd: "REPLACE_WITH_DATABASE_PASSWORD"
  dbase: "batcher"

s3store:
  host: "http://127.0.0.1:9000"
  region: "us-east-1"
  bucket: "batcher"
  accessKey: "REPLACE_WITH_S3_ACCESS_KEY"
  secretKey: "REPLACE_WITH_S3_SECRET_KEY"

# Development server only; the CLI does not need to start this server.
server:
  host: "127.0.0.1"
  port: 8886
```

Use the endpoint and region appropriate to your storage service, and create
the bucket before ingestion. Protect the configuration file, for example
with `chmod 600`. Database and S3 values are read literally: `${VARIABLE}`
substitution is not implemented for these fields.

Provide `OPENAI_API_KEY` through your execution environment or secret
management system. The provider adapter currently supports only `openai`.
`DOCPROC_HOME` affects the application-home setting used for the default JWK
path; it does not replace the configuration-file lookup described above.

Create the database and application role using your normal PostgreSQL
administration process, then apply the schema to a new database:

```bash
psql -h 127.0.0.1 -U batcher -d batcher \
  -v ON_ERROR_STOP=1 -f Support/defs.sql
```

[Support/defs.sql](Support/defs.sql) creates the `batcher` schema and requests
the `pgcrypto` extension. The executing role needs the relevant privileges.
This is a schema definition, not a versioned migration system: `CREATE TABLE
IF NOT EXISTS` will not upgrade existing table definitions.

## Run a first production

A **production** groups the requests rendered from one template and one
source file. Start with a small source and a distinct production name.

### 1. Create a template

Save this as `summary.jinja`:

```jinja
Summarize the supplied material in clear English.
Use a short paragraph followed by three key points.
Do not invent facts absent from the source.

Title: {{ meta.title }}
Source item {{ index }}:
{{ text }}
```

### 2. Create the source

Save this as `items.txt`. Each item has a metadata header and a body; both
header boundaries use the exact delimiter shown below.

```text
-*-*-*-*-*-*-*-*-*-
title = "Template reuse"
-*-*-*-*-*-*-*-*-*-
A reusable template keeps instructions consistent across many input items.
-*-*-*-*-*-*-*-*-*-
title = "Result provenance"
-*-*-*-*-*-*-*-*-*-
Recording inputs and outputs makes generated material easier to review.
```

### 3. Register and preview

```bash
stack exec docproc -- template load summary.jinja summary
stack exec docproc -- template list
stack exec docproc -- producer summary items.txt summary-demo --dry-run
```

`template load` takes a file followed by a **template name**. In contrast,
`template delete` takes a template's **UUID**, obtained from `template list`.
Loading another template under the same name creates another stored version;
name-based lookup selects the most recently created one.

The preview prints each rendered request. **`--dry-run` skips request creation
and provider submission, but is not read-only:** ingestion uploads the source
to S3 first. With `--template`, it also stores the supplied template and its
database record. It still requires the database, S3 configuration, and provider
credentials. Keep preview logs private if inputs contain sensitive material.

### 4. Submit and collect

The following command submits paid API work:

```bash
stack exec docproc -- producer summary items.txt summary-demo \
  --provider openai --model gpt5.4-nano --batch-size 2
```

Model values here are **Batcher aliases**, not arbitrary provider model IDs.
For example, `gpt5.4-nano` maps to `gpt-5.4-nano`; it is also the current
default. See [Service.OpenAI.Models](src/Service/OpenAI/Models.hs) for the
accepted aliases and reasoning settings. An alias in the source does not
guarantee model access for your account. Check the generated request settings
against your provider configuration before scaling up.

`producer` starts submission, polling, and fetching engines, then waits for
**ten minutes**. It does not wait until every request finishes. Continue
polling and collecting already-submitted work with:

```bash
stack exec docproc -- receiver openai
```

`receiver` also runs for ten minutes and can be run again. It does not submit
requests still in `entered` state. Re-running `producer` with the same
production name is not a resume operation: production names are unique and
the command tries to create another production.

To inspect progress, run this query in the configured database:

```sql
SELECT r.state, count(*) AS requests
FROM batcher.requests AS r
JOIN batcher.productions AS p USING (production_id)
WHERE p.production_name = 'summary-demo'
GROUP BY r.state
ORDER BY r.state;
```

For this example, expect two request rows. A provider batch reporting
completion and Batcher having stored every answer are separate milestones.
Check collected results before export.

### 5. Export the answers

```bash
mkdir -p out
stack exec docproc -- process summary-demo text --output-dir out
stack exec docproc -- process summary-demo docx --output-dir out
```

The text files are named `summary-demo_1.txt`, `summary-demo_2.txt`, and so on;
DOCX uses the same basename. Results are selected in source-item order.
Export reads whatever results currently exist, so an incomplete production
can produce an incomplete set of files. Repeated exports overwrite matching
output files.

## Templates and source data

### Template context

Templates use [Ginger](https://hackage.haskell.org/package/ginger), a Haskell
template engine with Jinja-style syntax.

| Variable | Meaning |
| --- | --- |
| `production.name` | Production name, or a generated UUID string when omitted |
| `index`, `item.index` | Source-item index, starting at 1 |
| `text`, `item.content` | Source-item body |
| `meta`, `item.meta` | Parsed header metadata |
| `header_toml`, `item.header_toml` | Original header text |

The source reader supports a small TOML-like subset: simple assignments,
quoted strings, numbers, booleans, and dotted keys. It is not a complete TOML
parser. Prefer simple scalar metadata; array handling is limited, especially
for quoted commas and nested values. Text before the first delimiter is
ignored, and input without delimiters produces no items.

Template includes resolve stored template names through PostgreSQL and S3.
The current resolver removes the first two characters of the include path;
use `./` before the registered name, for example:

```jinja
{% include "./shared-instructions" %}
```

### Shared context and cache boundaries

For a template with shared instructions, place this marker on its own line
between the shared prefix and the variable content:

```jinja
You are reviewing a Haskell module.
Explain its public interface and identify incomplete implementations.
{# BATCHER:CACHE_BREAKPOINT #}
Module {{ meta.module }}:
{{ text }}
```

The preprocessor recognizes the exact marker surrounded by LF newlines.
Ginger rendering records its output position without emitting marker text.
The renderer separates preceding context from the remaining user message;
context is stored in `batcher.memories`, linked to requests, and submitted as
developer-role text. Without a marker, the rendered template becomes the user
message.

Cache-key and request-field construction live in
[Service.Types](src/Service/Types.hs),
[Service.OpenAI.Cache](src/Service/OpenAI/Cache.hs), and
[Service.OpenAI](src/Service/OpenAI.hs). Keep stable instructions before the
boundary and changing item content after it. Treat this as an implementation
facility: provider acceptance, cache hits, retention, and savings require
verification. Multiple-context splitting and ordering are still being revised;
inspect the outgoing payload before relying on multiple boundaries.

## Commands

Use `stack exec docproc -- COMMAND ...`. Global options precede the command:

```bash
stack exec docproc -- --config /path/to/config.yaml --debug 1 template list
stack exec docproc -- producer --help
```

| Command | Purpose |
| --- | --- |
| `producer TEMPLATE_NAME SOURCE_FILE [PRODUCTION_NAME] [VERSION]` | Ingest, render, and start submit/poll/fetch engines |
| `receiver SERVICE_PROVIDER` | Poll and fetch existing provider batches |
| `process PRODUCTION_NAME OUTPUT_MODE [-o OUTPUT_DIR]` | Export stored answers; modes: `text`, `docx`, `code`, `htmljs` |
| `template load TEMPLATE_FILE_NAME TEMPLATE_ID` | Store a template; despite the argument label, `TEMPLATE_ID` here is its name |
| `template list [--filter PATTERN]` | List templates; filter uses SQL `ILIKE`, e.g. `'summary%'` |
| `template delete TEMPLATE_ID` | Delete a template by UUID; existing production references can prevent deletion |
| `load DOC_FILE PROMPTS_FILE OUT_FILE` | Legacy file-based batch submission; writes provider identifiers to `OUT_FILE` |
| `fetch OUT_FILE RESULTS_DIR` | Legacy retrieval using the identifiers written by `load` |
| `extract JSONL_FILE OUTPUT_PREFIX` | Extract response text and write an HTML document |
| `postp HTML_FILE` | Apply deliverable-reference annotations and styling; write to a sibling `New/` directory |
| `gendoc HTML_FILE OUTPUT_PREFIX` | Generate DOCX, intermediate LaTeX, and PDF |
| `imgtest IMAGE_PATH MODEL DETAIL` | Run the direct image-analysis experiment |
| `printtest INPUT_FILE OUTPUT_FILE` | Convert an image-test response JSON to Markdown |
| `server` | Start the experimental Servant HTTP server |
| `version` | Display version information |

`producer` accepts `--template PATH` (`-t`), `--provider` (`-p`), `--model`
(`-m`), `--batch-size` (`-b`), and `--dry-run` (`-n`). `--template` supplies a
new local template file, despite its current help text calling it an ID.
`VERSION` is parsed and printed but not persisted as a production version.

The parser also advertises `submit`, but `MainLogic` has no dispatch branch
for it. Use `producer` for the database-backed workflow.

### Output conventions

- `text` writes the stored answer unchanged.
- `docx` interprets the answer as Markdown and converts it with Pandoc.
- `code` extracts one file block per answer, using the format shown below.
- `htmljs` expects an HTML filename and fenced HTML block, followed by a
  JavaScript filename and fenced `js` or `javascript` block.

Example answer for `process ... code`:

````markdown
**File: `Example.hs`**

```haskell
module Example where

greeting :: String
greeting = "Hello from Batcher"
```
````

See [PostProc.Convert](src/PostProc/Convert.hs) for the exact extraction
parsers. The `code` parser expects the file heading at the start of the answer.

`code` uses paths supplied in the generated answer. Review those paths before
export and run extraction in an isolated workspace: the current implementation
does not enforce containment within `--output-dir`. Generated files are not
automatically compiled or executed.

### Legacy docproc workflow

The original `load` command accepts a reference document and blank-line-separated
prompt entries such as `1: Executive overview`. It rewrites each entry into a
drafting request and adds domain-specific legal-document instructions. Use
`producer` when you want control over the complete prompt.

`load` reads `OPENAI_MODEL`, `OPENAI_REASONING_EFFORT`, and optionally
`OPENAI_PROMPT_CACHE_KEY`. Those settings do not configure `producer`.
`fetch` writes individual JSON result files and a batch summary; it does not
populate the database-backed production tables.

The legacy `extract` command currently writes only `<prefix>.html`: its text
write is commented out even though its console message mentions `.txt`.
`postp` references Tailwind's CDN and `draftDlv_1.css`; the latter is not
included in this repository. `gendoc` supports `REFERENCE_DOCX`, `PDF_TEMPLATE`,
and `PDF_ENGINE`; its font setup is tailored to XeLaTeX and TeX Gyre fonts.

## Image-analysis experiment

`imgtest` makes a synchronous Responses API request independently of the batch
engines. Run it from the repository root because it reads
[Assets/kdofaContext_1.md](Assets/kdofaContext_1.md) relative to the current
directory. That file contains the art-analysis context; edit it if your
experiment needs different instructions.

```bash
stack exec docproc -- imgtest painting.jpg gpt-5.6-luna high
stack exec docproc -- printtest \
  painting.gpt-5.6-luna.high.response.json analysis.md
```

All three `imgtest` arguments are required. Unlike `producer`, `MODEL` is
passed directly to the provider. The local input checks accept JPEG, PNG, and
WebP, and detail values `low`, `high`, `original`, and `auto`; the selected
model must support the request settings.

The experiment embeds the image as a base64 data URL, requests 4,000 maximum
output tokens with reasoning effort `none` and `store: false`, measures elapsed
time, prints usage and output, and saves the full response JSON beside the
image. Repeating the same image/model/detail combination overwrites that file.

Cost figures use hard-coded price tables and are estimates, not billing
reconciliation. `imgtest` and `printtest` currently calculate cache-write costs
differently. `printtest` extracts only `output[0].content[0].text`, so it is not
a general Responses API output parser. It reports image quality as
`not recorded` when the response metadata lacks it; `imgtest` does not add that
metadata itself.

## Architecture

The database-backed workflow has five stages:

1. **Ingest:** store template/source bytes, create a production, render its
   source items, and insert requests and initial events.
2. **Submit:** claim `entered` requests, attach stored context, upload JSONL,
   create a provider batch, and record its request associations.
3. **Poll:** observe provider status and schedule completed batches through
   `batcher.fetch_outbox`.
4. **Fetch:** archive raw output in S3, match answers using request UUIDs in
   `custom_id`, store answer text and metadata, and mark requests completed.
5. **Export:** read a production's collected answers and materialize local
   files through `process`.

The request-state enum is `entered`, `submitted`, `completed`, or `cancelled`.
Provider batch failures currently map affected requests to `cancelled`, with
the failure reason retained in event details. Provider state, stored answers,
and local exports are separate layers of progress.

| Area | Source |
| --- | --- |
| CLI, configuration, dispatch | [Options](src/Options.hs), [Options.Cli](src/Options/Cli.hs), [MainLogic](src/MainLogic.hs) |
| Ingestion, source parsing, Ginger rendering | [Assets.Template](src/Assets/Template.hs) |
| S3 storage | [Assets.Storage](src/Assets/Storage.hs), [Assets.S3Ops](src/Assets/S3Ops.hs) |
| Queues and workers | [Engine.Runner](src/Engine/Runner.hs), [Engine.Submit](src/Engine/Submit.hs), [Engine.Poll](src/Engine/Poll.hs), [Engine.Fetch](src/Engine/Fetch.hs) |
| Provider adapter and payloads | [Service.Provider](src/Service/Provider.hs), [Service.OpenAI](src/Service/OpenAI.hs) |
| Typed SQL and schema | [DB.EngineStmt](src/DB/EngineStmt.hs), [DB.TemplateStmt](src/DB/TemplateStmt.hs), [Support/defs.sql](Support/defs.sql) |
| Output conversion | [PostProc.Convert](src/PostProc/Convert.hs), [Commands.GenDocs](src/Commands/GenDocs.hs) |
| Experimental HTTP interface | [Api.Routing.ClientRts](src/Api/Routing/ClientRts.hs), [Api.Routing.ClientHdl](src/Api/Routing/ClientHdl.hs) |

PostgreSQL holds assets' metadata, productions, requests, shared memories,
batch associations, events, answer text, and the fetch outbox. S3 holds template
and source bytes and archived raw results. Worker engines use bounded STM
queues, `async`, database leases, claim tokens, and `FOR UPDATE SKIP LOCKED`.

This is an application-specific workflow implementation. A shared FUDD HFSM
runtime, monitoring UI, and general multimodal batch interface remain design
directions; they are not implemented integrations in this repository.

## Operational limits

These constraints affect how the current code should be operated:

| Area | Current behavior and implication |
| --- | --- |
| Work selection | Submission claims eligible requests across the database, without filtering by the current production or stored model selection. Use a dedicated queue/database and consistent provider/model settings; do not treat concurrent producers as isolated workloads. |
| Scheduling | The default submit batch size is 10 requests. The CLI wires 10 workers and queue depth 100 per engine, with 60-second claims. Most settings are in `Commands.Producer` and `Commands.Receiver`, not YAML. Batch size is a request count, not a token budget. |
| Provider payload | Batch requests currently set `max_output_tokens` to 200,000. Model limits and cache-policy fields require validation; these are not exposed as general CLI tuning options. |
| Recovery | Leases and an outbox support recovery, but do not provide exactly-once submission. A provider acceptance followed by a local failure can leave ambiguous work. Poll completion and fetch-outbox insertion use separate transactions. Reconcile provider and database records before replaying. |
| Result completeness | Error-line handling and missing-answer reconciliation are incomplete. A batch completion event or fetch count is insufficient proof that every source item has an answer. |
| Storage failures | `Assets.Storage.storeS3` currently discards the upload result before returning a locator. Verify object availability when diagnosing ingestion or archival failures. |
| HTTP server | Several routes return placeholder data, authorization enforcement is incomplete, and authentication paths log sensitive values. Keep `server` in a trusted development environment. It does not start the batch engines. |
| Automation | Some command failures are printed without a failing exit status. Verify expected records and output files rather than relying only on the process exit code. |

Back up PostgreSQL and S3 together: database locators refer to stored objects.
Prompts and results can contain sensitive content, so include the database,
bucket, preview logs, and exported files in your access and retention controls.

## Development and contributions

Use [GitHub issues](https://github.com/whatsupfudd/ai_batcher/issues) for bug
reports and proposed changes, and submit focused pull requests with the
problem, intended behavior, and verification steps. Include the commit,
command, and a minimal redacted input when reporting a failure.

```bash
stack build
stack test
```

The current [test/Spec.hs](test/Spec.hs) only prints
`Test suite not yet implemented`; a successful test command provides no
functional coverage.

Contributions should support FUDD's **SPPM** goals:

- **Security:** enforce authorization, remove secret logging, validate output
  paths, and propagate storage failures.
- **Productivity:** keep command examples accurate, improve actionable errors,
  and make preview and resume behavior explicit.
- **Performance:** measure queue behavior, token budgets, provider limits, and
  cache effectiveness before changing concurrency defaults.
- **Maintainability:** preserve the separation of ingestion, execution,
  provider adapters, SQL, and output conversion; add regression coverage for
  source parsing, context ordering, retries, and result completeness.

Follow the existing Haskell style, including `OverloadedRecordDot` and typed
Hasql statements. Treat [package.yaml](package.yaml) as the Hpack package
definition and keep the generated Cabal file consistent. Documentation and
examples should change alongside behavior.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
