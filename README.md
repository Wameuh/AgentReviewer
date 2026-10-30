# AgentReviewer

Self-hosted GitHub PR review bot. A watcher polls a repository every minute;
when someone posts a bare trigger comment (e.g. `/agent review`) on an open
PR, an AI agent session is spawned in a dedicated git worktree, reviews the
diff, and posts a `## Verdict: …` review back as a PR comment.

Two spawn backends:

- **daemon mode** — review sessions are created on an
  [AgentControl](https://github.com/Wameuh/AgentControl) daemon
  (`new_session`, prompt delivered inline, sidebar group `AgentReviewer`),
  tracked via `list_sessions`, timed out via `kill_session`, and deleted
  once done. The watcher itself can run as a daemon `process` component.
- **systemd mode** (legacy) — each review is a transient
  `systemd-run --user` unit running any CLI agent with a prompt file.

Everything — watched repo, trigger command, agent, parallelism, timeouts —
lives in one TOML config file. Works with any agent the daemon can spawn
(daemon mode) or any CLI that accepts a prompt file (systemd mode).

## How it works

```
agentreviewer-watch.timer        or        agentcontrol-daemon
(OnCalendar=minutely)                process component: agent-reviewer --watch
        │                                        │
        ▼                                        ▼
             agent-reviewer (one poll cycle)
        │
        ├─ gh api: open PRs + issue/review comments since last scan
        ├─ match bare trigger comments, dedup by comment id (state.json)
        ├─ post an ack on the PR ("Launching / queued / duplicate")
        └─ for each queued request (up to max_parallel concurrent):
             git worktree add → fetch pull/<N>/head → checkout --detach
             ├─ daemon:  new_session {agent, cwd, title, model, prompt,
             │                      origin: "component:agentreviewer",
             │                      group} → set_session_group
             └─ systemd: systemd-run --user agentreviewer-<pr>-<cid>-<ts>
                           agent --prompt-file <generated prompt> …
        └─ per-review worktrees are removed once their review ends
```

- **One pending request per PR** — a second trigger while one is queued or
  running gets a "duplicate" reply.
- **Recovery** — if a review unit dies without posting (reboot, OOM), the
  request is re-queued automatically (up to `max_attempts`).
- **Follow-up reviews** — if the PR was already reviewed, the generated
  prompt scopes the agent to the commits pushed since the last verdict.
- **Verdict contract** — the review comment must start with
  `## Verdict: APPROVED | CONDITIONAL | REJECTED` and end with the
  `<!-- agentreviewer -->` marker; the watcher uses that to confirm posting.

## Requirements

- Python ≥ 3.11 (`tomllib`), `git`, `gh` (authenticated, `api` + `pr`)
- daemon mode: a reachable `agentcontrol-daemon` (unix socket or loopback
  TCP — nothing else, the watcher speaks NDJSON directly, `acctl` not
  needed). The daemon profile used must already be trusted.
- systemd mode: `systemd --user` with `systemd-run` (Linux) and a CLI
  agent (default: `devin`) able to read a prompt file and use `gh`.

## Install

As a managed component / CLI tool:

```sh
uv tool install git+https://github.com/Wameuh/AgentReviewer
```

Or the classic systemd layout:

```sh
./install.sh        # script → ~/.local/bin, units → ~/.config/systemd/user,
                    # example config → ~/.config/agentreviewer/config.toml
```

Then:

1. Edit `~/.config/agentreviewer/config.toml` — set `repo`, `repo_dir`,
   `worktree_root`, `trigger`, and the `[agent]` section. In daemon mode,
   set `mode = "daemon"` (or rely on `AGENTCONTROL_SOCK` being injected
   when running as a component) and optionally `daemon`, `group`.
2. Seed existing comments so they don't all trigger at once:
   `agent-reviewer --seed`
3. Enable: `systemctl --user enable --now agentreviewer-watch.timer`
   (systemd scheduling — both spawn modes work), or run the watcher as a
   daemon `process` component: `agent-reviewer --watch` gets
   `AGENTCONTROL_SOCK`/`AGENTCONTROL_COMPONENT` injected automatically,
   logs under `<daemon-root>/components/`, restarts on failure.

Logs: `journalctl --user -u agentreviewer-watch` plus
`<state_dir>/watcher.log`. Daemon mode: review sessions are visible in the
AgentControl client (group `AgentReviewer`) and deleted after completion
unless `keep_sessions = true`; systemd mode: `journalctl --user -u
'agentreviewer-*'`.

Manual cycle / testing without spawning:

```sh
agent-reviewer --config ~/.config/agentreviewer/config.toml --dry-run
```

## Configuration

See `config.example.toml` — every option is commented. Highlights:

| Key                | Default          | Meaning                                    |
|--------------------|------------------|--------------------------------------------|
| `repo`             | — (required)     | `owner/name` to watch                      |
| `repo_dir`         | — (required)     | local clone used for `git worktree add`    |
| `worktree_root`    | — (required)     | per-review worktrees are created here      |
| `trigger`          | `/agent review`  | bare comment that requests a review        |
| `max_parallel`     | `1`              | concurrent review sessions                 |
| `review_timeout`   | `3h`             | per-review hard timeout                    |
| `max_attempts`     | `5`              | respawn attempts before `failed`           |
| `unit_prefix`      | `agentreviewer`  | transient unit name prefix                 |
| `marker`           | `agentreviewer`  | HTML marker ending the review comment      |
| `post_acks`        | `true`           | post queue/launch acknowledgements         |
| `mode`             | auto             | `daemon` \| `systemd` spawn backend        |
| `daemon`           | env/default      | endpoint: `unix`, `unix:/path`, `tcp:h:p`  |
| `group`            | `AgentReviewer`  | sidebar group for review sessions          |
| `keep_sessions`    | `false`          | keep session+ journal after `done`         |
| `session_title`    | `review {repo} #{pr}` | title template (`{repo}` `{pr}` `{unit}` `{cid}`) |
| `[agent] command`  | `devin`          | agent executable / daemon profile basename |
| `[agent] profile`  | basename(command)| daemon profile for `new_session`           |
| `[agent] args`     | devin-style      | argv — systemd mode only                   |
| `[agent] model`    | `swe-2-max`      | `{model}` placeholder / session model      |
| `[agent] env`      | `{}`             | extra env vars — systemd mode only         |
| `[agent] path_extra` | `[]`           | extra PATH entries — systemd mode only     |

Watching several repos: deploy one `config.toml` + scheduling pair per
repo, each with a distinct `unit_prefix`, `state_dir`, and `worktree_root`.
In daemon mode the sessions of several watchers share the `AgentReviewer`
group — worktrees and state stay per-watcher.

## Notes

- Daemon mode assumes the configured agent profile is already trusted —
  permissions/workspace trust are owned by the daemon's profile, not by
  this watcher. `[agent] args/env/path_extra` are systemd-mode only.
- Review agents run `systemd-run --user … --collect` transient units — they
  do not survive a reboot; the queue does (state lives in `state_dir`).
- `repo_dir` is only used to mint worktrees — keep it clean; reviews run in
  their own checkouts under `worktree_root`, never in the main clone.
- `worktree_root` and `repo_dir` must be writable by the watcher; the shipped
  unit uses `ProtectSystem=full` (system dirs read-only) — home and other
  paths stay writable.
