# zello

A small local voice-message service for macOS. One Zello Work network, one channel,
and one durable SQLite queue. Local processes exchange text and JSON; the service
handles voice transport, transcription, and speech synthesis.

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

Every invocation creates missing private data directories and the configuration
example, then loads **only `~/.config/zello/.env`**. The working directory's `.env`
and exported environment variables do not override it. Missing credentials do
not prevent reading the queue. The service loads configuration at startup;
restart it after editing credentials or models.

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

## Commands

| Command | Result |
| --- | --- |
| `zello service` | Runs until Ctrl-C or SIGTERM; diagnostics on stderr and in the log |
| `zello status [--json]` | `connected` or `disconnected`; JSON also reports whether the service is running and whether it has used native transcription |
| `zello count [--json]` | Number of unread incoming messages ready to read |
| `zello inbox [--json]` | Readable unread messages, or a JSON array |
| `zello peek [--json]` | Oldest unread message as JSON, without consuming it |
| `zello consume <id> [--json]` | Atomically consumes that unread incoming message; prints its ID |
| `zello next [--json]` | Atomically consumes and prints the oldest unread message as JSON |
| `zello wait [--json]` | Blocks, then atomically consumes and prints one message as JSON |
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

## Service behavior

SQLite in WAL mode is the durable shared state. A private Unix socket supplies
live health, outgoing wakeups, and incoming notifications. A file lock allows
only one service. There is no listening TCP port. If the service is absent,
`wait` checks SQLite every 500 ms; while it is running, socket waits block and
are refreshed at most every 30 seconds.

Receive, transcription, outgoing delivery, and IPC run independently. WebSocket
failures reconnect with jittered backoff from roughly 1 to 30 seconds. Sending
starts only after successful authentication and an online channel notification.

Incoming audio is preserved before transcription. The service requests native
transcriptions and accepts a complete, nonempty event whose `stream_id` or
`streamId` identifies the corresponding incoming stream. Events without a usable
stream ID are logged as uncorrelated and use the saved-audio fallback; arrival
order does not establish which message was transcribed. The service waits up to
three seconds after audio completion for an identified native event, then uses
OpenAI audio transcription with `gpt-4o-mini-transcribe` (or the configured model).
A partial native event is never treated as a complete message.
The first completed transcript wins, so late events cannot rewrite consumed text.
No chat or completion API is involved.

A pending transcription stays durable but is excluded from the unread inbox
until text is ready. Failed transcription attempts retry with backoff up to five
minutes, including after restart. A missing OpenAI key leaves the audio pending
unless a native transcript with a matching stream ID arrives. IDs and deferred
errors are recorded in the log and can be inspected with `show`.
Broken/incomplete received streams are
retained with an error and withheld from normal consumption.

Typed Zello messages are stored with their text and ready state in one database
transaction. They appear in the same unread inbox immediately, survive restart,
and need neither audio nor transcription. Native diagnostics log only selected
metadata and text lengths; nested translations and unknown values are omitted.

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
