# Presenting

## Before you go on

1. `./run.sh up --preset <yours>` at least 10 minutes early (first run downloads the image). Check
   `./run.sh status` shows every node live.
2. Stage on the big screen at `http://localhost:8088`, fullscreen (`F`). Presenter on your laptop or phone at
   `/presenter`. Pick the theme for the room: dark for stages, light for bright booths (`T`).
3. Start **Payments** (Presenter → Workload) a minute before you begin so the chart has history.
4. Decide on latency: realistic makes locality stories obvious; off makes everything snappier.
5. On shared Wi-Fi, set `CONTROL_TOKEN` (and `TRIGGER_TOKEN` for hardware).

### Keeping the cloud illusion

The stage never says where it runs: no hostnames, ports or connection strings, and provisioning reads like a
cloud ("Allocating capacity in us-east-1…"). Keep the presenter console and terminals off the big screen. If you
want a cloud-looking address in the browser bar, add a hosts-file entry such as
`127.0.0.1 console.global-payments.example` and open `http://console.global-payments.example:8088`.

## 5 minutes: "you can't break it"

1. **Explainer** (optional): `/explainer/chaos` from 0:42 to 1:04, the region outage chapter.
2. Point at the stage: three regions, every node serving, payments flowing, money lost $0.
3. Scenario **Survive a region outage**: take down the primary region. While the tiles go red, say what is
   happening: leases move, that region's Payments workers reconnect to the nearest region (its chip under the
   chart reads "via" until it heals), retries absorb the blip.
4. Read the KPIs out loud: unavailable ranges 0, money lost $0, recovered in a few seconds.
5. Bring the region back and let the audience watch it rejoin.

## 15 minutes: resilience plus multi-region

1. 5-minute flow above.
2. **ZONE vs REGION survival**: same failure, before and after one `ALTER DATABASE`. Under ZONE the payments
   homed in the lost region stall while the other regions keep paying; under REGION every region keeps flowing.
3. **REGIONAL BY ROW**: the latency heatmap. The diagonal is local (about 1 ms); everything else pays the round trip.
4. **Elastic scale**: add a node live, then decommission it and watch replicas drain first.
5. Close on the Continuum explainer (`/explainer/continuum`, when your install includes it) if the audience is executive.

## 30 minutes: the full tour

1. 15-minute flow.
2. **Follower reads** and **GLOBAL tables**: when to use which.
3. **Isolation levels: write skew**: the classic bug READ COMMITTED allows and SERIALIZABLE prevents.
4. **Online schema change under load**: zero payment errors while a column and an index are added.
5. **Changefeeds**: crash the node running the feed; it resumes from its checkpoint.
6. **Split-brain test**: an isolated region refuses to commit alone; nothing diverges.
7. **Time travel**: undo an accidental `DELETE` with `AS OF SYSTEM TIME`.
8. Q&A with the chaos controls open: let the audience pick what to break.

## Booth

1. Presenter → Booth: enable booth mode, check auto-heal (45 s works well), reset counters.
2. Map your hardware (see [hardware-triggers.md](hardware-triggers.md)) and test every trigger with **Fire**.
3. Leave Payments running. Between visitors, press `E` for the explainer playlist; any key returns to the stage.
4. Visitors can hit as hard as they like: booth mode only picks victims the survival goal tolerates and refuses
   anything that would make data unavailable.

## With Banko AI Assistant

Run Banko's loader against `postgresql://root@localhost:26257/defaultdb?sslmode=disable`, then break regions on
the stage: the chart and KPIs include Banko's traffic. Realistic latency slows the loader; switch latency off in
the presenter if that matters for your story. Rebuilding with another preset wipes `defaultdb`: restart Banko.

## If something goes wrong

- A step fails: the scenario card shows the error. End the scenario (it cleans up), recover everything, and
  continue.
- A node is stuck: Presenter → Chaos → Recover everything.
- The cluster is in a bad state: Presenter → Cluster → rebuild the preset (about 1–2 minutes), or `./run.sh reset`.
