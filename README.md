# zello

A small local voice-message service for macOS. Each profile has one Zello Work
network, one channel, and one durable SQLite queue. Local processes exchange text
and JSON; the service handles voice transport, transcription, and speech synthesis.

```text
local processes → text → zello service → voice → Zello Work → iPhone
local processes ← text ← zello service ← voice ← Zello Work ← iPhone
```

## Build and configure

Requires Go 1.25+ to build and `ffmpeg` on PATH at runtime. SQLite is embedded;
there is no database server or separate codec library to install.

```sh
# Only if ffmpeg is missing:
brew install ffmpeg

cd ~/Code/github.com/sadasant/zello
go build -trimpath -o zello .
mkdir -p ~/.local/bin
install -m 755 zello ~/.local/bin/zello
export PATH="$HOME/.local/bin:$PATH"
zello count
```

Without `--profile`, commands use **`~/.config/zello/.env`** and create missing
private data directories and the configuration example. The working directory's
`.env` and exported environment variables do not override it. Missing credentials
do not prevent reading the default queue. The service loads configuration at
startup; restart it after editing credentials or models.

To create the configuration without overwriting an existing file:

```sh
(umask 077; cp -n ~/.config/zello/.env.example ~/.config/zello/.env)
chmod 600 ~/.config/zello/.env
# Edit ~/.config/zello/.env privately with your editor.
```

Use the fields in [.env.example](.env.example). Set the Zello **Work username,
password, and channel**. The account must be able to join and speak in that channel.
The default network is `sadasant`, using `wss://zellowork.io/ws/sadasant`.
ElevenLabs needs an API key and voice ID for outgoing speech. Configure an OpenAI
key for incoming transcription when native transcription is unavailable or its
events omit a stream ID.

Credentials are never included in application logs or provider error messages.
Provider response bodies are not used as diagnostics. Configuration and data
directories are mode 0700; database, audio files, socket, and new log files are
0600. Treat the stored voice and text as private local data. Neither the real
configuration nor generated data belongs in Git.

## Saved profiles

Save a named configuration once, then let local processes select it:

```sh
zello profile save Peter
zello --profile Peter service
# In another terminal:
zello --profile Peter send "Hello Daniel."
zello --profile Peter wait --json
zello profile list --json
```

On a terminal, `profile save` prompts for the Zello account and channel, plus
optional transcription and speech-provider settings. Password and API-key input
is hidden. Empty model/network answers use the same defaults as `.env`.
No credentials are accepted as command-line arguments or printed in results.

Processes can register profiles by passing JSON over stdin:

```sh
zello profile save Peter --stdin < /path/to/private-profile.json
```

The JSON object uses the same uppercase setting names as [.env.example](.env.example).
For example, a minimal profile contains `ZELLO_USERNAME`, `ZELLO_PASSWORD`, and
`ZELLO_CHANNEL`, each with a string value. Include the OpenAI and ElevenLabs fields
when needed. Unknown fields, duplicate keys, and invalid configurations fail
without echoing the submitted values. Saving returns only the canonical profile
name; `--json` returns `{"profile":"peter"}`.

Names are case-insensitive ASCII letters/digits, `_`, and `-`, beginning with a
letter or digit, at most 32 characters. `Peter` and `peter` select the same profile.
`default` selects the original `.env` configuration and is reserved from saving.
Registration is atomic and create-only: a second save to the same name fails,
including concurrent saves. To use different credentials, register a new name.
There is no automatic replacement of a configuration that processes already use.

Named credentials are stored as JSON in
`~/.config/zello/profiles/peter.json` with mode **0600**, inside private directories.
These are local files, not an encrypted keychain. The profile contains its own
complete settings; omitted optional keys do not inherit credentials from `.env`
or exported environment variables. A missing or invalid named profile fails
instead of selecting the default account.

Each named profile keeps its database, audio, log, socket, and service lock under
`~/.local/share/zello/profiles/peter/`. Processes selecting the same name share
that queue and one service. Different names can run services concurrently, with
separate queues; a message ID from one profile is unavailable in another. Start
one `zello --profile NAME service` for each profile you want connected.

`--profile NAME` and `--profile=NAME` work before or after an ordinary command.
Use `--` when message text itself resembles an option:

```sh
zello --profile Peter send -- "--profile"
```

Profile-management commands take their name as an argument, without `--profile`.
The existing commands and data paths remain the default when no profile is selected.

## Commands

| Command | Result |
| --- | --- |
| `zello profile save <name> [--stdin] [--json]` | Register credentials privately; existing names are not overwritten |
| `zello profile list [--json]` | List profile names, including the built-in `default`, without credentials |
| `zello service` | Runs until Ctrl-C or SIGTERM; diagnostics on stderr and in the log |
| `zello status [--json]` | `connected` or `disconnected`; JSON also reports whether the service is running and whether it has used native transcription |
| `zello count [--json]` | Number of unread incoming messages ready to read |
| `zello inbox [--json]` | Readable unread messages, or a JSON array |
| `zello peek [--json]` | Oldest unread message as JSON, without consuming it |
| `zello consume <id> [--json]` | Atomically consumes that unread incoming message; prints its ID |
| `zello next [--json]` | Atomically consumes and prints the oldest unread message as JSON |
| `zello wait [--json]` | Blocks, then atomically consumes and prints one message as JSON |
| `zello subscribe [--json]` | Streams unread-ID snapshots over a persistent connection; never consumes |
| `zello send "text" [--json]` | Durably enqueues outgoing text and immediately returns its ID |
| `echo "text" \| zello send` | Same enqueue behavior, reading stdin |
| `zello show <id> [--json]` | Durable message state as JSON |

`peek`, `next`, `wait`, and `show` always produce JSON; `--json` is accepted for
consistency. Message JSON includes `id`, `direction`, `from`, `channel`, `text`,
`status`, and `created_at`; `consumed_at`, `reply_to`, and `error` appear when set.
The CLI does not expose audio formats or paths.

A successful operation exits 0. Empty `peek`/`next`, missing IDs, repeated
consumption, invalid input, and disconnected health checks exit 1. `wait` keeps
waiting on an empty inbox. An interrupted command exits 130. Normal results go
to stdout and diagnostics go to stderr. `status` prints its result even when
it exits 1. An empty JSON inbox is `[]`.

Multiple consumers can wait concurrently; each incoming message is consumed
once. Use `peek` plus `consume` when processing must succeed before consumption.
`next` and `wait` consume before printing, so a consumer crash or broken output
pipe after consumption does not restore the message automatically.

### Event subscriptions

`zello subscribe --json` prints newline-delimited JSON: an initial snapshot of
readable unread IDs, then another snapshot when the service makes incoming text
available. Each line has the form `{"type":"inbox","ids":["message-id"]}`;
an empty snapshot has `"ids":[]`. Transcripts and audio paths are not included.
Use `show <id>` to read and `consume <id>` after processing succeeds.

The connection blocks indefinitely while idle. There is no timer, heartbeat,
database polling, or message consumption in the subscription. Every subscriber
gets its own snapshots; these are advisory, not exclusive claims. Concurrent
consumers can consume an ID before you read it. Consumption alone does not emit
a notification. Deduplicate IDs durably if you forward notifications elsewhere.

The service must be running. A disconnection exits nonzero; callers should
reconnect with backoff on failure. Each new connection takes a fresh snapshot,
including unread messages received during downtime. Slow readers may be
disconnected after a blocked write. This keeps reconnect/recovery explicit while
ordinary idle connections stay open. The existing consuming `wait` command keeps
its short-polling fallback for use when the service is unavailable.

## Service behavior

SQLite in WAL mode is the durable shared state. A private Unix socket supplies
live health, outgoing wakeups, and incoming notifications. A file lock allows
only one service per profile. There is no listening TCP port. If the service is absent,
`wait` checks SQLite every 500 ms; while it is running, socket waits block and
are refreshed at most every 30 seconds.

Receive, transcription, outgoing delivery, and IPC run independently. WebSocket
failures reconnect with jittered backoff from roughly 1 to 30 seconds. Sending
starts only after successful authentication and an online channel notification.

Incoming audio is preserved before transcription. The service requests native
transcriptions and accepts a complete, nonempty event whose `stream_id` or
`streamId` identifies the corresponding incoming stream. When Zello omits the ID,
the service can infer an association with one recent recording using the bounded
rule below. It waits up to three seconds after audio completion, then uses OpenAI
audio transcription with `gpt-4o-mini-transcribe` (or the configured model).
A partial or empty native event is never treated as a complete message.
The first completed transcript wins, so late events cannot rewrite consumed text.
No chat or completion API is involved.

### Native transcripts without a stream ID

Some observed Zello Work incoming transcription events omit both stream-ID
spellings. In the observed network, their `sender` value also equals the channel
name rather than the audio sender's username. Outgoing transcription events have
included `streamId`. These are observations, not protocol guarantees; neither
sender matching nor FIFO pairing is assumed.

The service applies this single-candidate heuristic inside each connection:

1. Observe the start and completion of one valid incoming recording, with no
   other recording or outgoing transmission involved. Hold it as the sole
   candidate for at most three seconds after completion.
2. Accept a complete, nonempty unidentified transcript during that window if
   no other voice stream has started. Record the successful association as
   `source=native_inferred` in the log.
3. If another recording starts first, immediately make the old recording
   eligible for OpenAI fallback and wake the transcription worker. The new
   recording is also ineligible for inferred matching. Its audio still records
   normally and becomes eligible for fallback when complete. Receiving audio
   never waits for an OpenAI request to finish.
4. During ambiguity, discard unidentified native transcripts. Resume optimistic
   matching only for a recording that starts after five quiet seconds with no
   active incoming or outgoing voice. Voice starts/stops and unidentified events
   extend that interval. A recording that started during quarantine stays
   ineligible even if it ends after the interval.

An expired candidate, a rejected recording, an unidentified partial/orphan event,
or an outgoing send attempt also triggers quarantine. After a successful native
association, the five-second quiet interval guards against duplicate events being
assigned to the next recording. For timeouts, the interval starts at the expired
deadline. No background timer is needed for this bookkeeping: event handlers
compare timestamps, while the existing worker handles durable fallback deadlines.

This trades some certainty for faster conversational transcripts. Zello has not
provided an upper bound on native-transcription delay: an old unidentified event
arriving after the quiet interval while a new candidate is pending can still be
misassociated. Five seconds is a local heuristic, not a Zello delivery guarantee.
The original audio is retained. Explicitly identified native events remain usable
during quarantine, and matching state never crosses a connection boundary.

Successful completion logs distinguish `source=native_stream_id`,
`source=native_inferred`, and `source=openai`. These are diagnostic log fields;
the CLI message schema is unchanged. `native_transcription_observed` includes
successful inferred matches. Restarting loses only temporary matching state:
saved pending audio remains eligible for OpenAI, and unread text stays durable.

### Durable inbox and delivery

A pending transcription stays durable but is excluded from the unread inbox
until text is ready. Failed transcription attempts retry with backoff up to five
minutes, including after restart. A missing OpenAI key leaves the audio pending
unless a native transcript is accepted under the rules above. IDs and deferred
errors are recorded in the log and can be inspected with `show`.
Broken/incomplete received streams are
retained with an error and withheld from normal consumption.

Typed Zello messages are stored with their text and ready state in one database
transaction. They appear in the same unread inbox immediately, survive restart,
and need neither audio nor transcription. Native diagnostics log selected typed
metadata, text lengths, and bounded event structure. Sender/channel/identifier
equality can be compared using opaque tokens within one service process; the
random token key is not persisted. Speech, translations, arbitrary string values,
and unknown field names are not copied into the log. These diagnostic tokens are
never used to establish a transcript's identity.

Outgoing state progresses through `queued → synthesizing → sending → sent`.
Ordinary ElevenLabs HTTP TTS uses the configured voice and `eleven_flash_v2_5` by
default. `ffmpeg` handles audio conversion internally. A busy channel or a
connection lost before transmission leaves the message queued; available
synthesized audio is reused. TTS failures and permanent send rejections become
`failed` and can be inspected with `show`.

Restart keeps queued, unread, and pending incoming messages. Interrupted
synthesis returns to `queued`. Interrupted `sending` becomes `failed` with an
explicit uncertain-delivery error: retrying automatically could speak the same
message twice. Failed messages remain available; sending the text again creates
a new ID. Changing the configured channel does not redirect previously queued
messages: a channel mismatch fails explicitly.

## Local files

These paths describe the default profile. Named profiles use the configuration
and data directories described above.

| Path | Purpose |
| --- | --- |
| `~/.config/zello/.env` | User-supplied configuration; not created with secret values |
| `~/.config/zello/.env.example` | Generated configuration template |
| `~/.local/share/zello/messages.db` | Durable message and retry state |
| `~/.local/share/zello/audio/` | Original incoming and synthesized outgoing audio, named by message ID |
| `~/.local/share/zello/zello.log` | Service diagnostics without message bodies or credentials |
| `~/.local/share/zello/zello.sock` | Private live service IPC |
| `~/.local/share/zello/service.lock` | Exclusive service lifetime lock |

Files are retained; there is no automatic deletion or log rotation. Finished
incoming voice is preserved as playable Ogg Opus without changing the original
packet bytes. An invalid/unsupported stream is retained as a private raw
`.zello` capture instead. Temporary transcription WAV files are removed after
each attempt. A hard process kill may leave temporary audio files behind.

## First live acceptance test

These commands require real credentials and an iPhone joined to the same channel.
Local automated tests do **not** establish handset playback or network-specific
native transcription support.

Terminal 1:

```sh
export PATH="$HOME/.local/bin:$PATH"
zello service
```

Terminal 2, once connected:

```sh
export PATH="$HOME/.local/bin:$PATH"
zello status --json
id=$(zello send "Testing one two three.")
zello show "$id"
```

Hear “Testing one two three.” on the iPhone. Run `zello show "$id"` again after
playback to check `sent`.

With an initially empty inbox, speak “Can you hear me?” from the iPhone. After
transcription completes (typically a few seconds):

```sh
zello count                    # 1
zello peek --json              # text contains "Can you hear me?"
zello next --json              # same message, now consumed
zello count                    # 0
zello wait --json               # blocks until you speak another message
```

Speak another message; `wait` should return its text and consume it. To verify
persistence, stop the service, enqueue text with `send`, restart it, and check
that the queued text is spoken. An unread incoming message should remain in
`peek` across the same restart.

## Verification and protocol boundaries

```sh
go test -race ./...
go vet ./...
```

Tests use temporary databases, Unix sockets, fake WebSocket/HTTP endpoints, and
real ffmpeg conversion. They do not contact paid APIs. The only direct Go
dependencies are Gorilla WebSocket, modernc SQLite, and godotenv.

The [official Channel API](https://github.com/zelloptt/zello-channel-api/blob/main/API.md)
documents native `on_transcription` events, but availability depends on the
network. The service observes actual completed native events rather than
assuming that requesting the feature makes it available. Zello's repository
labels the API beta.

`sent` means the audio and stream termination were written to Zello, not proof of
playback on an iPhone. No handset playback receipt or disconnected-history
replay is used. Voice broadcast while disconnected cannot be recovered by this
service. Only received audio can be persisted.

Implementation limits are ten minutes and 16 MiB of incoming packet data per
message, with at most eight simultaneous incoming streams; outgoing speech is
also limited to ten minutes. Text is limited to 40,000 characters. These are
resource bounds, not claims about network limits. Voice may include a few
milliseconds of encoder delay or tail padding because the Channel API carries
no original encoder trim metadata. The protocol's illustrative codec header
and packet duration disagree; the implementation validates actual packet timing.

Provider references: [OpenAI audio transcription](https://developers.openai.com/api/docs/guides/speech-to-text)
and [ElevenLabs HTTP text to speech](https://elevenlabs.io/docs/api-reference/text-to-speech/convert).
