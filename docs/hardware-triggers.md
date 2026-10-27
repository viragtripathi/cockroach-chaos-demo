# Hardware triggers

Plastic cockroaches and a hammer, a big red button, a foot pedal: anything that can press a key or send an HTTP
request can break (and heal) the cluster.

## How it works

A **trigger** is an id mapped to an action and a target. Edit them in Presenter → Booth.

| Field | Values |
|---|---|
| action | `crash`, `stop`, `partition`, `brownout`, `zone-outage`, `region-outage`, `isolate`, `recover-all` |
| target | `random-safe` (default), `random-node`, `random-region`, `node`, `zone`, `region`, `all` |
| value | the node, zone or region name when the target is `node`, `zone` or `region` |

Shipped defaults:

| Id | Action | Key |
|---|---|---|
| `roach-1` | crash a random node the survival goal tolerates | `1` |
| `roach-2` | region outage of a random region | `2` |
| `big-red` | recover everything | `9` |

In **booth mode**, random targets always become `random-safe` and anything that would make data unavailable is
refused, so visitors can hit as hard as they like. Auto-heal recovers everything after the quiet period.

## Option 1: keyboard (no code)

The stage maps keys `1`–`9` to triggers. Any USB device that types a digit works:

- **Makey Makey**: wire each plastic roach (with foil or conductive tape underneath) to an input remapped to a
  digit with the Makey Makey remap page, and the hammer (also foil-wrapped) to ground. A hit closes the circuit
  and "types" the digit.
- **USB big red button or foot pedal**: most can be programmed to send a key; set it to `9` for "recover everything".

Keep the stage browser focused (kiosk or fullscreen).

## Option 2: HTTP (ESP32, Raspberry Pi, anything on the network)

    POST http://<panel-host>:8088/api/triggers/<id>
    X-Trigger-Token: <TRIGGER_TOKEN>      # only if TRIGGER_TOKEN is set in .env

The response says what happened:

```json
{"ok": true, "trigger": "roach-1", "action": "crash", "kind": "node", "value": "us-west-2b-n2", "targets": ["us-west-2b-n2"]}
```

`ok: false` with a `message` means it was refused (booth mode safety) or nothing was left to hit.

Try it from a terminal:

    curl -X POST -H "X-Trigger-Token: $TRIGGER_TOKEN" http://localhost:8088/api/triggers/roach-1

### ESP32 (Arduino) example

A piezo or a switch under each roach on GPIO 13 and 14, debounced:

```cpp
#include <WiFi.h>
#include <HTTPClient.h>

const char* WIFI_SSID = "booth";
const char* WIFI_PASS = "...";
const char* PANEL = "http://192.168.1.50:8088";   // the laptop running ./run.sh up
const char* TOKEN = "change-me";                    // TRIGGER_TOKEN from .env
const int PINS[] = {13, 14};
const char* IDS[] = {"roach-1", "roach-2"};
unsigned long last[2] = {0, 0};

void fire(const char* id) {
  HTTPClient http;
  http.begin(String(PANEL) + "/api/triggers/" + id);
  http.addHeader("X-Trigger-Token", TOKEN);
  int code = http.POST("");
  Serial.printf("%s -> %d %s\n", id, code, http.getString().c_str());
  http.end();
}

void setup() {
  Serial.begin(115200);
  for (int pin : PINS) pinMode(pin, INPUT_PULLUP);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) delay(200);
}

void loop() {
  for (int i = 0; i < 2; i++) {
    if (digitalRead(PINS[i]) == LOW && millis() - last[i] > 1500) {  // 1.5 s debounce per roach
      last[i] = millis();
      fire(IDS[i]);
    }
  }
}
```

## Security

The panel listens on your laptop's network interfaces. On shared Wi-Fi, set both tokens in `.env`:

    CONTROL_TOKEN=some-long-random-string     # presenter changes; open /presenter?token=... once per browser
    TRIGGER_TOKEN=another-random-string       # hardware triggers

Without `TRIGGER_TOKEN`, triggers follow `CONTROL_TOKEN` (a browser unlocked through `/presenter?token=…` can
fire them from the stage keys). With `TRIGGER_TOKEN` set, the stage keys and the presenter's **Fire** buttons only
work in a browser unlocked with `CONTROL_TOKEN`, so set both.
