// Plain-language explanations shown as hover tooltips on the stage and in the presenter console.
// One place for the wording, so both screens teach the same thing. Shown on the stage: keep it cloud-neutral.

const COPIES = {
  region: '5 copies of every range, spread so that losing a whole region still leaves a majority',
  zone: '3 copies of every range in the primary region, one per availability zone',
  none: '3 copies of every range on different nodes',
};

export const GLOSSARY = {
  // KPIs
  liveNodes: 'Live nodes: nodes whose liveness heartbeat is current. A node that misses its heartbeat for a few ' +
    'seconds counts as down, and the ranges it was leading get new leaseholders elsewhere.',
  unavailable: 'Unavailable ranges: ranges that have lost a majority of their copies, so they can\'t serve reads or ' +
    'writes. 0 means every piece of data is reachable. Under-replicated ranges are only missing a copy: they keep ' +
    'serving while CockroachDB re-creates it.',
  tps: 'Transactions per second from every client (the built-in Payments workload, Banko, anything else), measured ' +
    'by the cluster itself.',
  p99: 'p99 latency: 99% of SQL statements finished faster than this; the slowest 1% took longer. Tail latency ' +
    'is what users notice: it goes up with cross-region round trips and briefly during a failover. Below it: ' +
    'p50 (the typical statement) and p95 (all but the slowest 5%).',
  moneyLost: 'Money lost: the Payments workload moves money between 10,000 accounts that always add up to ' +
    '$10,000,000, and re-checks the total every 5 seconds. Any difference would be money lost or created by a ' +
    'failure. It stays at $0 because every transfer is one ACID transaction, and retries never apply a transfer twice.',
  recovery: 'Recovered in: time from the last failure until payments are back to at least 80% of their normal rate ' +
    'for two seconds. 0 s means throughput never dipped.',

  // header
  goal: (goal) => `Survival goal: the cluster keeps ${COPIES[goal] || COPIES.none}. ` + ({
    region: 'Writes keep committing even if an entire region goes down.',
    zone: 'Losing one zone is fine; losing the whole primary region stalls writes until it returns.',
  }[goal] || 'Losing one node is fine; losing two at once can make some data unavailable.'),
  incident: 'What is broken right now. Each failure is injected on purpose from the control panel.',
  booth: 'Booth mode: everything heals automatically after a quiet period, so visitors can keep breaking things. ' +
    'Random targets only pick nodes the survival goal can afford to lose.',

  // regions and zones
  primary: 'Primary region: home of the leaseholders for regional data, so reads and writes there are fastest. ' +
    'Other regions keep copies for survival and for local follower reads.',
  secondary: 'Secondary region: where the leaseholders move if the primary region goes down, so failover lands ' +
    'somewhere predictable.',
  regionState: {
    healthy: 'Healthy: every node in this region is serving.',
    degraded: 'Degraded: some nodes in this region are down or impaired. The region keeps serving with the rest.',
    down: 'Outage: every node in this region is down. Data stays available from the other regions as long as the ' +
      'survival goal allows it.',
    unknown: 'Status unknown: the cluster\'s health couldn\'t be read just now.',
  },
  zone: (zone) => `Availability zone ${zone}: an independent failure domain inside the region (separate power and ` +
    'network). CockroachDB spreads the copies of a range across zones.',

  // node states (tile labels)
  nodeState: {
    down: 'Down: this node isn\'t running or isn\'t heartbeating. Its leases moved to other nodes within seconds; ' +
      'if it stays down for over a minute, its copies are re-created elsewhere where placement rules allow.',
    partitioned: 'Partitioned: this node is running but cut off from the network: nobody can reach it and it can\'t ' +
      'reach anyone, like a pulled network cable.',
    isolated: 'Isolated: this node can\'t talk to the other regions (a split-brain test). Clients near it can still ' +
      'connect, but it can\'t commit anything without a majority, so the data never diverges.',
    brownout: 'Brownout: extra delay, jitter and packet loss on this node\'s network. Slow and flaky rather than ' +
      'dead: the hardest kind of failure to detect.',
    draining: 'Draining: being decommissioned. Its copies move to other nodes before it is removed, so no data is ' +
      'ever at risk.',
    joining: 'Joining: starting up and joining the cluster. It will take on copies and leases as data rebalances.',
    unknown: 'Telemetry delayed: this node\'s status couldn\'t be read just now.',
    missing: 'Allocating: this node is being created.',
  },

  // region buttons on the stage, and actions in the presenter
  action: {
    Recover: 'Recover: restart stopped nodes, reconnect partitioned ones and clear any injected network faults.',
    Outage: 'Region outage: kill every node in this region at once, like the whole region losing power.',
    Partition: 'Partition: disconnect every node in this region from the network.',
    Isolate: 'Isolate: cut this region off from the other regions while its local clients can still connect ' +
      '(split-brain test).',
    Brownout: 'Brownout: make this region\'s network slow and flaky (+700 ms, jitter, 2% packet loss) instead of ' +
      'killing it.',
    '+ Node': 'Add a node in this region. Copies of ranges and their leases rebalance onto it automatically.',
    '⋯': 'More: zone outages, isolate, make this the primary region, or remove the region.',
    'Zone outage': 'Zone outage: kill every node in this availability zone at once, like a whole datacenter losing ' +
      'power. With one node per zone and ZONE survival, the other zones still hold a majority of every range.',
    crash: 'Crash: kill the target instantly, with no warning (like a power cut).',
    outage: 'Outage: crash every node in the zone or region.',
    stop: 'Planned stop: shut the node down gracefully. It hands off its leases first, as in a rolling restart.',
    partition: 'Partition: disconnect the target from the network; it keeps running.',
    isolate: 'Isolate: cut a region off from the other regions; its local clients can still connect.',
    brownout: 'Brownout: add delay, jitter and packet loss to the target\'s network.',
    recover: 'Recover: undo every fault on the target: restart, reconnect and clear network faults.',
  },

  chart: 'Transactions per second, stacked by gateway region: the region of the node each client connected to. ' +
    'Red dashed lines mark failures, green lines mark recovery.',
  rate: (n) => `Transactions per second that clients sent through this node (it was their gateway): ${n}/s right ` +
    'now. Any node can serve any query; the load balancer spreads clients across healthy nodes.',
  replicas: (n, goal) => 'R (replicas): CockroachDB splits data into ranges and keeps several copies of each (3, or ' +
    `5 under REGION survival) on different nodes. This cluster keeps ${goal === 'region' ? '5, for REGION survival'
      : goal === 'zone' ? '3, for ZONE survival' : '3'}. This node holds ${n} of those copies.`,
  leases: (n) => 'L (leases): one copy of each range holds the lease. That node serves reads for the range and ' +
    `coordinates its writes. This node is the leaseholder for ${n} range${n === 1 ? '' : 's'}. When a node or ` +
    'region fails, its leases move to the survivors within seconds.',

  // presenter console
  model: 'Deployment model for a rebuild. Multi-region spreads the preset\'s regions across the map and survives ' +
    'losing a region. Single region uses the preset\'s primary region on its own, one node per availability zone, ' +
    'and survives losing a zone: the setup of customers who run in one region.',
  latency: 'Inter-region latency: delays traffic between regions according to their real distance (about 60 ms ' +
    'between US coasts, about 90 ms to Europe). Half and Double scale it; Off makes every region feel local.',
  makePrimary: 'Make primary: move the home of regional data, and its leaseholders, to this region.',
  addNode: 'Add node: start a new node in the region; copies and leases rebalance onto it.',
  addRegion: 'Add region: create the region\'s nodes and add it to both databases (chaos and defaultdb), so apps ' +
    'like Banko can use it right away.',
  rebuild: 'Rebuild: delete this cluster and build a fresh one from the preset. All data is wiped.',
  customPreset: 'A preset is a small YAML file: display name, regions (any names), nodes per region and survival ' +
    'goal. Saved presets appear in the list above.',
  targetKind: 'What to hit: one node, every node in a zone or region, or the whole cluster (Everything only makes ' +
    'sense with Recover).',
  brownoutParams: 'Brownout settings: extra one-way delay, random jitter on top of it, and the share of packets to drop.',
  decommission: 'Decommission: move every copy off the node, then remove it. The safe way to shrink a cluster.',
  removeRegion: 'Remove region: drop it from both databases, move its data away, then remove its nodes.',
  workers: 'Workers: parallel connections sending transfers. Each paces itself so all of them together hit the target rate.',
  rate_target: 'Target rate: transfers per second the workload aims for. Each transfer is one ACID transaction ' +
    'debiting one account and crediting another.',
  p50: 'p50: half of the transfers finished faster than this (the typical case).',
  p95: 'p95: 95% of the transfers finished faster than this; only 1 in 20 took longer.',
  retries: 'Retries: transfers retried after a serialization conflict (SQLSTATE 40001) or a transient error, with ' +
    'exponential backoff and jitter.',
  transient: 'Transient errors retried: errors by class that a retry fixed (ambiguous result 40003, connection, ' +
    'timeout). They are expected while a node or region fails.',
  failed: 'Failed transfers: transfers that gave up after every retry. It should stay at 0.',
  dedup: 'Applied before a retry: a retry found the transfer had already committed (its id was already there), so ' +
    'it wasn\'t applied twice. This is why retrying an ambiguous commit is safe.',
  boothEnabled: 'Booth mode: visitors can\'t break the cluster for good. Random targets only pick nodes the ' +
    'survival goal can afford to lose, unsafe hits are refused, and everything heals after the quiet period.',
  autoHeal: 'Auto-heal: recover everything this many seconds after the last hit.',
  triggerTargets: 'Targets: random-safe picks any node the survival goal can afford to lose; random-node and ' +
    'random-region pick any (booth mode makes them safe); node, zone and region need a name in Value.',
};
