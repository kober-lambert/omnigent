# Preferences and config defaults

Users can save local model and harness defaults, override them for a project,
and choose how an individual session runs. Native startup arguments also come
from local configuration. Shared workspace preferences have a resolver
and an opt-in server API for durable edits and conflicts. Automatic client sync
and shared/machine scope controls are not available through the CLI or UI yet.
An explicit Python client can persist offline edits and synchronize them on reconnect.

## Sub-features

- `user-defaults`: set, list, and remove defaults on this machine; cover empty
  config, invalid keys, legacy scalar harness settings, and custom config homes.
- `project-defaults`: project YAML overrides user YAML; cover absent files,
  per-harness fields, explicit clears, and scalar harness replacement.
- `launch-choice`: explicit invocation choices take precedence over defaults;
  keep configured startup arguments ordered before invocation arguments.
- `inspect-host-defaults`: inspect the selected host's startup settings and
  installed plugins, including unavailable or older hosts.
- `session-choice`: choose a harness/model for a new session or change a
  supported setting in an existing session; keep the two composers distinct.
- `shared-scopes`: resolver only; requested values and their sources remain
  distinct across shared, machine, project, and session scopes.
- `shared-operations`: opt-in server API; read/save all four groups, retry an
  edit, remove a preference, inspect competing edits, and resolve them explicitly.
- `shared-client`: explicit Python client; preserve offline edits across process
  restarts, retry uncertain deliveries, refresh the cache, and surface conflicts.

## How to get to it (user POV)

**Local CLI and files:** `omnigent config set --global model=<model>` or
`harness=<harness>` saves a user default. Omit `--global` to save in the current
project. Use `omnigent config list` to inspect the files and
`omnigent config unset [--global] model` to remove a value. Startup commands
and argument lists are configured in the files' per-harness mappings.

**Launch:** run `omnigent run` with an agent/default agent and optional model
or harness flags, or use a native command such as `omnigent codex` with
pass-through arguments. The native entry points and prerequisites are listed in
[Native harnesses](./native-harnesses.md).

**Web and desktop:** the new-session composer, in-session configuration menu,
native terminal controls, and fork dialog have separate selection paths; see
[Composer](./composer.md). Settings → Harnesses → select a host → a Claude or
Codex card/gear → Settings shows read-only startup configuration. The Plugins
tab shows inventory; both are covered in [Native harnesses](./native-harnesses.md).

**Shared operations API:** on an isolated server configured with
`shared_preferences_authority_id`, use `GET /v1/preferences/shared` and
`POST /v1/preferences/shared/operations` with the server's normal authentication.
The [API guide](../docs/shared-preferences-api.md) covers requests, conflict
resolution, identity, and limits. These endpoints do not adopt preferences into
launchers or turn on sync.

**Embedded client:** use the [client API](../docs/shared-preferences-client.md)
with a private cache file, an expected account/workspace/deployment identity,
and the caller's authenticated HTTP client. Queue explicit edits locally, then
synchronize on reconnect. The caller schedules synchronization; launchers, the
host daemon, and the settings UI do not call this API automatically yet.

**Shared scope controls:** the CLI and UI do not yet offer importing/reimporting
preferences, saving shared or machine defaults, removing a machine override,
resolving conflicts, or continuing with temporary defaults. The API above can
save shared defaults and resolve conflicts. Add CLI/UI routes and their journey
tests here as those controls are implemented.

## Driving it with the repro environment

Preconditions: use an isolated config home and project; never edit your usual
config to verify this recipe. The CLI subprocess tests below own their
environment and use a local mock model where needed. They need no browser or
shared repro instance. For the linked browser/native recipes, follow their
`verify-env` and harness prerequisites.

**Local defaults and launch (own environment):**

```sh
uv run --no-sync pytest tests/e2e/omnigent/test_config_defaults_e2e.py --reruns 0 -q
```

This verifies config write/list/unset across processes, rejection of an unknown
key without persisting it, and a default-agent launch with a mock reply.

**Project and invocation compatibility (component tests):**

```sh
uv run --no-sync pytest tests/test_config.py tests/test_harness_startup_config.py \
  tests/cli/test_cli.py --reruns 0 -q
```

- `user-defaults`, `project-defaults`: the config loader tests cover file
  precedence and clears; CLI tests cover writes, removal, inspection, and scalar
  harness migration. Removing a project value allows the user default to apply.
  The adopted-preference clear matrix also covers aliases such as `claude` for
  `claude-sdk`: clearing the entry or its arguments removes inherited arguments,
  a command-only override keeps them, and a session choice can still override.
- `launch-choice`: CLI dispatch tests check explicit choices and Pi argument
  ordering. The native startup tests check Claude/Codex pass-through arguments
  remain unmerged until the runner, so the configured base is applied once.
  These are component checks, not real vendor-terminal launches.
- `inspect-host-defaults`, `session-choice`: use the linked Native harnesses and
  Composer recipes for the actual settings, terminal, new-session, in-session,
  fork, and mobile surfaces. Their results do not establish shared-preference
  persistence or isolation across machines.

**Shared scopes (component coverage only):**

```sh
uv run --no-sync pytest tests/test_preferences.py tests/test_config.py --reruns 0 -q
```

The matrix checks precedence for all four groups, explicit disabled plugins
and empty argument lists, removal of overrides, and rejection of mismatched
workspace/account/machine contexts. The YAML-to-launch test checks source
explanations, project precedence, and unchanged files. No real client currently
adopts these layers. Before activation, add cross-machine, restart, conflict,
offline, native-plugin, and managed-startup journeys with explicit coverage for
each user entry point.

**Shared operations (own environment, real server processes):**

```sh
uv run --no-sync pytest tests/e2e/omnigent/test_shared_preferences_e2e.py --reruns 0 -q -rs
```

This drives two isolated CLI servers sharing a SQLite database through actual
HTTP requests (or another isolated backend via `OMNIGENT_TEST_DB_URI`). It checks
all preference groups, conflicting edits, explicit
resolution, retries, user isolation, deletion, and persistence after restart.
No services or providers are mocked; no model is called. This verifies the local
OSS API, not managed deployment or actual native application of preferences.
The store matrix also runs on PostgreSQL, MySQL, and CockroachDB:

```sh
uv run --no-sync pytest tests/stores/test_preference_store.py \
  tests/server/routes/test_preferences.py tests/test_preference_sync.py --reruns 0 -q
```

For later slices, continue live verification of each newly connected path on an
isolated deployment. Record the candidate/runtime versions, configuration,
observed results, any mocked services, and remaining entry-point gaps.

**Durable client (own environment):** the same server test file also runs
`test_durable_preference_clients_offline_reconnect_and_restart`. It launches
separate client processes with private caches, queues all four groups offline,
resolves competing edits through two server replicas, injects a lost response
after a real commit, stops both servers, and starts a fresh replica to verify
retry/deletion convergence. It also rejects a duplicate conflict resolution,
explicitly discards that blocked edit, and delivers the remaining queued work.
The lost-response fault is injected; HTTP and SQL are real. This is local OSS
client/API evidence, not automatic host sync, launcher adoption, or Arclet proof.

```sh
uv run --no-sync pytest tests/test_preference_client.py --reruns 0 -q -rs
```

The component matrix covers causal offline edits, malformed or stale replies,
identity changes, cancellation, local writers, sync locking, and outbox limits.
Its HTTP transport is simulated; the mapped process test covers real transport.

For latency and contention use the existing
[benchmark runner](../dev/benchmarks/omnigent/README.md#shared-preferences).
Record key/history/client counts, backend/driver, feature gate, revision,
latency percentiles, errors, and HTTP requests per operation. Compare receipt
history sizes and existing server journeys with the API off/on; label injected
network delay separately from a real WAN. Performance numbers do not replace
the correctness scenarios above.

## Gotchas

- `--global` means this machine's user YAML, not a shared workspace write.
  Reading or editing YAML does not opt into shared preferences.
- `config list` displays user and project defaults under their file paths;
  seeing both rows does not mean both values are active.
- Server-stored project defaults used by the new-session composer are separate
  from local `.omnigent/config.yaml`. The stubbed browser requests in
  `tests/e2e_ui/start_session/test_project_config_prefill.py` exercise the former,
  not reading host YAML or syncing workspace preferences.
- A missing setting, an explicit project clear, an empty argument list, and a
  disabled plugin have different meanings. A scalar project harness replaces
  the whole lower harness value; a mapping merges per field.
- Startup commands and credentials stay local. Selecting a harness or copying
  its arguments does not install the binary or make referenced paths available.
- Read-only plugin inventory is not proof that enable/disable preferences can
  be applied independently in concurrent workspaces. Session import review is
  also separate from the future preference-import flow.
- Resolver tests and feature-map contract checks do not prove browser behavior,
  native policy enforcement, remote provisioning, or sync convergence.
