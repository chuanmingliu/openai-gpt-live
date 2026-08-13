# Realtime voice demo — quick start

Run everything from the repository root:

```sh
make demo-wizard
```

The interactive menu offers:

1. **Full durable demo** — starts private PostgreSQL, speech, gateway, worker,
   Web Search, and the browser client.
2. **Client only** — starts the browser client for an existing loopback
   gateway.
3. **Check status** — checks browser, gateway, and worker readiness.
4. **Stop demo** — stops the wizard-owned browser and, when authorized, the
   durable Compose services without deleting the PostgreSQL volume.

## Recommended first run

Choose **Full durable demo**. The wizard asks for:

- hidden: a disposable PostgreSQL password, a new local gateway bearer, the
  speech-service credential, and the DeepSeek credential;
- visible: the reviewed speech image and the absolute path to its non-secret
  configuration file.

Secret values exist only in the current wizard session and the child services
it starts. The wizard does not write `.env` or credential files.

When startup succeeds, open:

**[http://127.0.0.1:3000/](http://127.0.0.1:3000/)**

Grant browser **microphone and speaker** permission, select **Start
listening**, and speak. For Web Search, open **Tools**, enable Web Search, run
**Test**, then **Save** before starting the voice session.

Run `make demo-wizard` again and choose **Check status** or **Stop demo**.

## Client-only shortcut

Choose **Client only** when the gateway is already running. Enter its local
bearer, loopback WebSocket URL, and advertised model. Defaults are:

```text
ws://127.0.0.1:18080/v1/realtime
gpt-live-simulator
```

The browser never receives the bearer; the loopback BFF injects it into the
gateway connection.

## Requirements and help

Full mode requires Go, Docker Compose, `curl`, `jq`, `openssl`, a reviewed
speech-service image/configuration, and valid speech/DeepSeek credentials.
Client-only mode requires Go, `curl`, and an already-running loopback gateway.

For manual audio/barge-in acceptance, idle-pool checks, troubleshooting, and
security details, use the
[complete realtime browser operator guide](../../docs/demo/realtime-browser-client.md).

This is a loopback development demo, not production or release evidence.
