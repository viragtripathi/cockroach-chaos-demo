# Your own presets

The presenter console's **Cluster → Custom preset** editor saves presets here, and
`./run.sh up --preset <name>` finds `presets/custom/<name>.yaml` as well as the built-in ones.

Git ignores everything in this folder except this file, so presets named after a customer stay on your machine.
To ship a preset with the demo for everyone, add it to `presets/` instead.
