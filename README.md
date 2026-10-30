# AgentReviewer

Self-hosted GitHub PR review bot. A systemd `--user` timer polls a repository
every minute; when someone posts a bare trigger comment (e.g. `/agent review`)
on an open PR, an AI agent session is spawned in a dedicated git worktree,
reviews the diff, and posts a `## Verdict: …` review back as a PR comment.

Everything — watched repo, trigger command, agent binary/model, parallelism,
timeouts — lives in one TOML config file. Works with any CLI agent that
accepts a prompt file and has `gh` access.

## How it works

```
agentreviewer-watch.timer  (OnCalendar=minutely)
        │
        ▼
agentreviewer-watch.service  →  agent-reviewer (one poll cycle)
        │
        ├─ gh api: open PRs + issue/review comments since last scan
        ├─ match bare trigger comments, dedup by comment id (state.json)
        ├─ post an ack on the PR ("Launching / queued / duplicate")
        └─ for each queued request (up to max_parallel concurrent):
             git worktree add → fetch pull/<N>/head → checkout --detach
             systemd-run --user agentreviewer-<pr>-<cid>-<ts>
               agent --prompt-file <generated prompt> …
        └─ per-review worktrees are removed once their unit ends
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
- `systemd --user` with `systemd-run` (Linux)
- A CLI agent (default: `devin`) able to read a prompt file and use `gh`

## Install

```sh
./install.sh        # script → ~/.local/bin, units → ~/.config/systemd/user,
                    # example config → ~/.config/agentreviewer/config.toml
```

Then:

1. Edit `~/.config/agentreviewer/config.toml` — set `repo`, `repo_dir`,
   `worktree_root`, `trigger`, and the `[agent]` section.
2. Seed existing comments so they don't all trigger at once:
   `agent-reviewer --seed`
3. Enable: `systemctl --user enable --now agentreviewer-watch.timer`

Logs: `journalctl --user -u agentreviewer-watch` plus
`<state_dir>/watcher.log` (review agents log under their own unit,
`journalctl --user -u 'agentreviewer-*'`).

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
| `[agent] command`  | `devin`          | review agent executable                    |
| `[agent] args`     | devin-style      | argv; `{prompt_file} {model} {repo} {pr} {worktree}` placeholders |
| `[agent] model`    | `swe-2-max`      | substituted into `{model}`                 |
| `[agent] env`      | `{}`             | extra env vars for the review unit         |
| `[agent] path_extra` | `[]`           | extra PATH entries for the agent           |

Watching several repos: deploy one `config.toml` + systemd unit pair per
repo, each with a distinct `unit_prefix`, `state_dir`, and `worktree_root`.

## Notes

- Review agents run `systemd-run --user … --collect` transient units — they
  do not survive a reboot; the queue does (state lives in `state_dir`).
- `repo_dir` is only used to mint worktrees — keep it clean; reviews run in
  their own checkouts under `worktree_root`, never in the main clone.
- `worktree_root` and `repo_dir` must be writable by the watcher; the shipped
  unit uses `ProtectSystem=full` (system dirs read-only) — home and other
  paths stay writable.
