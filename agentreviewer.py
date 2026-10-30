#!/usr/bin/env python3
"""AgentReviewer — watch GitHub PRs for review-trigger comments.

Each new bare trigger comment (e.g. `/agent review`) on an open PR is
queued; up to `max_parallel` agent sessions run concurrently per watched
repository, each in its own git worktree with the PR head checked out.
The agent posts the review back as a PR comment. A single instance can
watch several repositories (`repos = [ { … } ]`, per-entry overrides
falling back to the top-level defaults; state is namespaced per repo).

Reviews run either as transient `systemd-run --user` units (legacy mode)
or as AgentControl daemon sessions (daemon mode — `new_session` NDJSON
requests, tracked via `list_sessions`). Designed to run every minute from
agentreviewer-watch.timer (systemd --user) or as a `process` component
managed by agentcontrol-daemon (`--watch` loop, AGENTCONTROL_SOCK
injected). Everything is configured via a TOML file — see
config.example.toml.

Usage:
  agent-reviewer [--config PATH]   one poll cycle (used by the systemd unit)
  agent-reviewer --config PATH --watch [--interval SEC]
                                   poll in-process every SEC seconds
  agent-reviewer --config PATH --dry-run
                                   detect pending comments, build prompts,
                                   create no worktree, spawn nothing
  agent-reviewer --config PATH --seed
                                   mark all current matching comments as
                                   processed, spawn nothing
  agent-reviewer review --path DIR --base REF [--out FILE]
                                   [--timeout SEC] [--detach]
                                   review the local diff REF...HEAD in an
                                   existing worktree — daemon session when
                                   an endpoint is reachable, foreground
                                   agent run otherwise. Exit code:
                                   0=APPROVED 4=CONDITIONAL 5=REJECTED
                                   3=no verdict/timeout 2=usage/infra error
"""

import argparse
import fcntl
import glob
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from datetime import datetime, timedelta, timezone

__version__ = "0.3.0"

DEFAULT_CONFIG = "~/.config/agentreviewer/config.toml"
DEFAULT_DAEMON_SOCK = "~/.agentcontrol/daemon.sock"
DEFAULT_AGENT_ARGS = [
    "--model", "{model}",
    "--permission-mode", "dangerous",
    "--respect-workspace-trust", "false",
    "--prompt-file", "{prompt_file}",
    "--print",
]
# Daemon session states meaning "the review is over" — anything else
# (starting, idle, working, running, waiting_input, compacting, asleep)
# counts as alive; a session absent from list_sessions is finished too.
DEAD_STATES = {"dead", "interrupted"}
# `origin` sent on new_session — marks sessions as spawned by this
# watcher (client shows "via component:agentreviewer").
DAEMON_ORIGIN = "component:agentreviewer"


def expand(p):
    return os.path.abspath(os.path.expandvars(os.path.expanduser(p)))


def parse_duration(s, default=10800.0):
    """systemd-style time span ("3h", "90min", "1h30m", bare seconds)."""
    units = {"us": 1e-6, "ms": 1e-3, "s": 1, "sec": 1, "m": 60, "min": 60,
             "h": 3600, "hr": 3600, "d": 86400, "w": 604800}
    total, n = 0.0, 0
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(us|ms|min|sec|hr|s|m|h|d|w)",
                         str(s), re.IGNORECASE):
        total += float(m.group(1)) * units[m.group(2).lower()]
        n += 1
    if n:
        return total
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _agent_fields(a):
    return {
        "agent_cmd": str(a.get("command", "devin")),
        "agent_profile": str(a.get(
            "profile",
            str(a.get("command", "devin")).rsplit("/", 1)[-1])),
        "agent_args": [str(x) for x in a.get("args", DEFAULT_AGENT_ARGS)],
        "model": str(a.get("model", "swe-2-max")),
        "agent_env": {str(k): str(v) for k, v in a.get("env", {}).items()},
        "path_extra": [expand(p) for p in a.get("path_extra", [])],
    }


def load_config(path, allow_missing=False):
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f)
    except FileNotFoundError:
        if not allow_missing:
            raise
        raw = {}

    cfg = {
        "repo": raw.get("repo"),
        "repo_dir": raw.get("repo_dir"),
        "worktree_root": raw.get("worktree_root"),
        "trigger": str(raw.get("trigger", "/agent review")),
        "max_parallel": max(1, int(raw.get("max_parallel", 1))),
        "review_timeout": str(raw.get("review_timeout", "3h")),
        "max_attempts": int(raw.get("max_attempts", 5)),
        "since_overlap_sec": int(raw.get("since_overlap_sec", 600)),
        "unit_prefix": re.sub(r"[^a-zA-Z0-9_.-]", "-",
                              str(raw.get("unit_prefix", "agentreviewer"))),
        "marker": str(raw.get("marker", "agentreviewer")),
        "post_acks": bool(raw.get("post_acks", True)),
        "mode": str(raw.get("mode", "") or "").lower(),
        "daemon": str(raw.get("daemon", "") or ""),
        "group": str(raw.get("group", "AgentReviewer")),
        "keep_sessions": bool(raw.get("keep_sessions", False)),
        "session_title": str(raw.get(
            "session_title", "review {repo} #{pr}")),
        **_agent_fields(raw.get("agent", {})),
        "state_dir": expand(os.environ.get(
            "AGENTREVIEWER_STATE_DIR",
            raw.get("state_dir", "~/.local/state/agentreviewer"))),
    }
    if cfg["mode"] not in ("", "daemon", "systemd"):
        sys.exit(f"agent-reviewer: config '{path}': mode must be "
                 "'daemon' or 'systemd'")
    try:
        cfg["session_title"].format(repo="", pr=0, unit="", cid=0)
    except (KeyError, IndexError, ValueError) as e:
        sys.exit(f"agent-reviewer: config '{path}': bad session_title "
                 f"template: {e}")
    cfg["state_file"] = os.path.join(cfg["state_dir"], "state.json")
    cfg["log_file"] = os.path.join(cfg["state_dir"], "watcher.log")
    cfg["lock_file"] = os.path.join(cfg["state_dir"], "lock")

    cfg["repos"] = []
    if cfg["repo"]:
        cfg["repos"].append(_resolve_repo(cfg, cfg, path, "primary"))
    for i, ent in enumerate(raw.get("repos", [])):
        if not isinstance(ent, dict):
            sys.exit(f"agent-reviewer: config '{path}': repos[{i}] "
                     "must be a table")
        cfg["repos"].append(_resolve_repo(cfg, ent, path, i))
    return cfg


# Keys a `repos[]` entry may override; everything else stays global.
REPO_OVERRIDES = ("worktree_root", "trigger", "group", "session_title",
                  "max_parallel", "keep_sessions", "post_acks", "marker",
                  "review_timeout", "max_attempts", "since_overlap_sec",
                  "unit_prefix")


def _resolve_repo(g, ent, path, idx):
    """Fully-resolved per-repo config: global defaults + entry overrides."""
    where = "primary repo (top-level keys)" if idx == "primary" \
        else f"repos[{idx}]"

    def need(key):
        v = ent.get(key) or g.get(key)
        if not v:
            sys.exit(f"agent-reviewer: config '{path}': {where} "
                     f"missing required '{key}'")
        return v
    rc = dict(g)
    rc["repo"] = str(need("repo"))
    rc["repo_dir"] = expand(str(need("repo_dir")))
    rc["worktree_root"] = expand(str(need("worktree_root")))
    for k in REPO_OVERRIDES:
        if ent.get(k) is not None:
            rc[k] = ent[k]
    rc["trigger"] = str(rc["trigger"])
    rc["group"] = str(rc["group"])
    rc["session_title"] = str(rc["session_title"])
    rc["max_parallel"] = max(1, int(rc["max_parallel"]))
    rc["keep_sessions"] = bool(rc["keep_sessions"])
    rc["post_acks"] = bool(rc["post_acks"])
    rc["marker"] = str(rc["marker"])
    rc["review_timeout"] = str(rc["review_timeout"])
    rc["review_timeout_sec"] = parse_duration(rc["review_timeout"])
    rc["max_attempts"] = int(rc["max_attempts"])
    rc["since_overlap_sec"] = int(rc["since_overlap_sec"])
    rc["unit_prefix"] = re.sub(r"[^a-zA-Z0-9_.-]", "-",
                               str(rc["unit_prefix"]))
    trig = r"\s+".join(re.escape(t) for t in rc["trigger"].split())
    rc["trigger_re"] = re.compile(trig + r"\s*", re.IGNORECASE)
    # [agent] fields: entry keys merge over the global table.
    for k in ("agent_cmd", "agent_profile", "agent_args",
              "model", "agent_env", "path_extra"):
        rc[k] = g[k]
    ea = ent.get("agent")
    if isinstance(ea, dict):
        if ea.get("command") is not None:
            rc["agent_cmd"] = str(ea["command"])
            rc["agent_profile"] = rc["agent_cmd"].rsplit("/", 1)[-1]
        if ea.get("profile") is not None:
            rc["agent_profile"] = str(ea["profile"])
        if ea.get("args") is not None:
            rc["agent_args"] = [str(x) for x in ea["args"]]
        if ea.get("model") is not None:
            rc["model"] = str(ea["model"])
        if ea.get("env") is not None:
            rc["agent_env"] = {str(k): str(v)
                               for k, v in ea["env"].items()}
        if ea.get("path_extra") is not None:
            rc["path_extra"] = [expand(p) for p in ea["path_extra"]]
    rc["slug"] = re.sub(r"[^a-zA-Z0-9_.-]", "-",
                        rc["repo"].split("/")[-1])
    try:
        rc["session_title"].format(repo="", pr=0, unit="", cid=0)
    except (KeyError, IndexError, ValueError) as e:
        sys.exit(f"agent-reviewer: config '{path}': repos[{idx}] "
                 f"bad session_title template: {e}")
    return rc


CFG = None  # set in cli()
DAEMON = None       # live AgentControl client for this cycle, or None
USE_DAEMON = False  # spawn reviews via the daemon this cycle


def log(msg):
    line = f"{datetime.now().astimezone().isoformat(timespec='seconds')} {msg}"
    print(line, flush=True)
    try:
        os.makedirs(CFG["state_dir"], exist_ok=True)
        if os.path.exists(CFG["log_file"]) \
                and os.path.getsize(CFG["log_file"]) > 2_000_000:
            with open(CFG["log_file"], "rb") as f:
                f.seek(-500_000, os.SEEK_END)
                tail = f.read()
            with open(CFG["log_file"], "wb") as f:
                f.write(tail)
        with open(CFG["log_file"], "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def gh_api(endpoint):
    out = subprocess.run(
        ["gh", "api", "--paginate", "--slurp", endpoint],
        capture_output=True, text=True, timeout=60,
    )
    if out.returncode != 0:
        raise RuntimeError(f"gh api {endpoint} failed: {out.stderr.strip()[:300]}")
    items = []
    for page in json.loads(out.stdout or "[]"):
        items.extend(page if isinstance(page, list) else [page])
    return items


def load_state():
    """State file = {"repos": {"owner/name": {"comments", "last_check"}}}.
    A legacy flat {"comments", "last_check"} layout migrates into the
    primary repo's namespace (configured repos[0])."""
    try:
        with open(CFG["state_file"]) as f:
            st = json.load(f)
    except (OSError, json.JSONDecodeError):
        st = {}
    if not isinstance(st, dict):
        st = {}
    repos = st.setdefault("repos", {})
    legacy = {k: st.pop(k) for k in ("comments", "last_check") if k in st}
    if legacy and CFG["repos"]:
        ns = repos.setdefault(CFG["repos"][0]["repo"], {})
        log(f"state: migrated legacy entries into "
            f"{CFG['repos'][0]['repo']} namespace")
        if "comments" in legacy:
            ns["comments"] = {**legacy["comments"],
                              **ns.get("comments", {})}
        ns.setdefault("last_check", legacy.get("last_check"))
    return st


def save_state(state):
    os.makedirs(CFG["state_dir"], exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=CFG["state_dir"],
                               prefix="state.", suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, CFG["state_file"])


def nvm_bin():
    dirs = sorted(glob.glob(os.path.expanduser("~/.nvm/versions/node/*/bin")))
    return dirs[-1] if dirs else None


def agent_desc(rc):
    d = rc["agent_profile"] if USE_DAEMON \
        else rc["agent_cmd"].rsplit("/", 1)[-1]
    return f"{d} ({rc['model']})" if rc["model"] else d


class DaemonError(Exception):
    """Connection failure or ok:false reply from agentcontrol-daemon."""


class Daemon:
    """Minimal AgentControl NDJSON client — one request per connection.

    Wire format: one JSON line {"id":N,"req":"...",...} out, one reply
    line {"id":N,"ok":true,"result":{...}} (or ok:false + "error") back.
    No auth — the daemon only listens on loopback / a unix socket.
    """

    def __init__(self, endpoints, timeout=15):
        self.endpoints = endpoints
        self.timeout = timeout
        self._seq = 0

    def _connect(self):
        err = None
        for kind, *addr in self.endpoints:
            try:
                if kind == "unix":
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.settimeout(self.timeout)
                    s.connect(addr[0])
                else:
                    s = socket.create_connection(
                        (addr[0], addr[1]), timeout=self.timeout)
                return s
            except OSError as e:
                err = e
        raise DaemonError(f"no endpoint reachable ({err})")

    def call(self, req, **fields):
        self._seq += 1
        msg = {"id": self._seq, "req": req, **fields}
        try:
            s = self._connect()
        except DaemonError:
            raise
        try:
            f = s.makefile("rwb")
            f.write(json.dumps(msg).encode() + b"\n")
            f.flush()
            line = f.readline()
        except OSError as e:
            raise DaemonError(f"{req}: {e}") from e
        finally:
            s.close()
        if not line:
            raise DaemonError(f"{req}: connection closed")
        try:
            r = json.loads(line)
        except json.JSONDecodeError as e:
            raise DaemonError(f"{req}: bad reply: {e}") from e
        if not r.get("ok"):
            raise DaemonError(r.get("error") or f"{req} failed")
        return r.get("result") or {}


def _endpoint(spec):
    if spec == "unix" or spec.startswith("unix:"):
        return ("unix", expand(spec[5:] or DEFAULT_DAEMON_SOCK))
    spec = spec[4:] if spec.startswith("tcp:") else spec
    host, _, port = spec.rpartition(":")
    return ("tcp", host or "127.0.0.1", int(port or "7331"))


def daemon_endpoints():
    """Candidates in order: the `daemon` config key is an exclusive
    override; otherwise $AGENTCONTROL_SOCK > $AGENTCONTROL_TCP >
    the default unix socket."""
    if CFG["daemon"]:
        return [_endpoint(CFG["daemon"])]
    eps = []
    if os.environ.get("AGENTCONTROL_SOCK"):
        eps.append(("unix", expand(os.environ["AGENTCONTROL_SOCK"])))
    if os.environ.get("AGENTCONTROL_TCP"):
        eps.append(_endpoint("tcp:" + os.environ["AGENTCONTROL_TCP"]))
    eps.append(("unix", expand(DEFAULT_DAEMON_SOCK)))
    seen = set()
    return [e for e in eps if not (e in seen or seen.add(e))]


def daemon_mode_enabled():
    """Resolved spawn mode: explicit `mode` wins; otherwise daemon when
    an endpoint is configured (`daemon` key or AGENTCONTROL_* env, which
    the daemon injects when supervising the watcher as a component)."""
    if CFG["mode"]:
        return CFG["mode"] == "daemon"
    return bool(CFG["daemon"] or os.environ.get("AGENTCONTROL_SOCK")
                or os.environ.get("AGENTCONTROL_TCP"))


def spawned_ref(e):
    """What identifies a running review: daemon session id or unit name."""
    return e.get("session") or e.get("unit")


def entry_alive(e, sessions, busy_units):
    """True = review still running, False = finished, None = unknown
    (backend unreachable — don't act on it this cycle)."""
    sid = e.get("session")
    if sid:
        if sessions is None:
            return None
        m = sessions.get(sid)
        return m is not None and m.get("state") not in DEAD_STATES
    return e.get("unit") in busy_units


def timed_out(rc, e):
    try:
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(
            e.get("ts") or e.get("created_at"))).total_seconds()
    except (TypeError, ValueError):
        return False
    return age > rc["review_timeout_sec"]


def active_review_units(prefix):
    try:
        out = subprocess.run(
            ["systemctl", "--user", "list-units", "--type=service",
             "--state=active,activating", "--no-legend", "--plain",
             f"{prefix}-*"],
            capture_output=True, text=True, timeout=30,
        )
    except OSError:
        return set()  # no systemd on this host (e.g. managed container)
    if out.returncode not in (0, 1):
        return set()
    return {l.split()[0].removesuffix(".service")
            for l in out.stdout.splitlines() if l.strip()}


def review_posted(rc, entry):
    """True: review landed on the PR. False: none found. None: check failed."""
    try:
        comments = gh_api(
            f"repos/{rc['repo']}/issues/{entry['pr_num']}"
            "/comments?per_page=100")
    except Exception as ex:
        log(f"review check failed for comment "
            f"{entry.get('comment', {}).get('id')} "
            f"(PR #{entry.get('pr_num')}): {ex}")
        return None
    trig = entry.get("created_at") or ""
    marker = f"<!-- {rc['marker']} -->"
    for c in comments:
        if (c.get("created_at") or "") <= trig:
            continue
        body = (c.get("body") or "").lstrip()
        if marker in body \
                or body.startswith("## Verdict") \
                or body.startswith("## Review"):
            return True
    return False


def fmt_age(ts):
    try:
        m = int((datetime.now(timezone.utc)
                 - datetime.fromisoformat(ts)).total_seconds() // 60)
        return f"{m}min" if m < 90 else f"{m // 60}h{m % 60:02d}m"
    except (TypeError, ValueError):
        return "?"


def build_prompt(rc, pr, comment, worktree, follow_up=False):
    repo = rc["repo"]
    body = (comment.get("body") or "").strip()
    n = pr["number"]
    header = f"""A user commented `{rc['trigger']}` on a pull request on GitHub. The full event details are included below.

Event details:
- Repository: {repo}
- Pull request: #{n} — {pr.get('title', '')}
- PR URL: {pr['html_url']}
- Comment author: @{comment.get('user', {}).get('login', 'unknown')}
- Comment URL: {comment.get('html_url', '')}
- Comment kind: {comment.get('_kind', 'issue')}
- Working directory: dedicated git worktree at {worktree} — the PR head is already checked out there (`git log -1` shows the PR head commit; do not create commits or push from it)
- Comment body:
{body}
"""
    if follow_up:
        analysis = f"""
Instructions:
1. Verify the comment was posted on a pull request (issue_comment events also fire on plain issues; if this is not a PR, stop without posting anything)
2. Read the PR title, body and follow-up comments for context (`gh pr view {n} -R {repo} --comments`)
3. This is a FOLLOW-UP review: this PR was already reviewed at least once. Find the previous review comment(s) on the PR posted in response to earlier `{rc['trigger']}` commands (they start with `## Verdict:` — older ones may use a `## Review` heading instead) and identify the commits pushed since the last one (`gh api repos/{repo}/pulls/{n}/commits` lists head SHAs and dates). If no previous review comment exists on the PR, review the full diff instead.
4. Read ONLY the diff introduced by those new commits (for example `git diff <head-sha-at-last-review>...HEAD` in the worktree — the current PR head is already checked out), not the whole PR diff. If there are no new commits since the last review, say so briefly — do not redo the review.
5. Analyze ONLY the new changes and their direct integration with the code they touch. Do NOT re-review code that was already reviewed, do NOT repeat earlier remarks, and do NOT comment on unrelated code — exception: you may still flag a genuinely blocking issue (security hole, data loss, broken build) still present.
"""
    else:
        analysis = f"""
Instructions:
1. Verify the comment was posted on a pull request (issue_comment events also fire on plain issues; if this is not a PR, stop without posting anything)
2. Read the PR title, body and any follow-up comments for context (`gh pr view {n} -R {repo} --comments`)
3. Read the changes diff of the PR (`gh pr diff {n} -R {repo}`)
4. Analyze ONLY the changes in this PR and their direct integration with the code they touch — do NOT make remarks about unrelated code, files, or the repository in general.
5. Review the PR: provide relevant comments to increase security, quality of code, maintainability — strictly scoped to the diff.
"""
    return header + analysis + f"""
Your review comment MUST begin with a verdict line, exactly one of:
- `## Verdict: APPROVED` — no blocking issue; the PR can be merged
- `## Verdict: CONDITIONAL` — mergeable once the listed conditions are addressed
- `## Verdict: REJECTED` — blocking issues must be fixed before merge

Then add the review as a comment on the PR (`gh pr comment {n} -R {repo} --body-file <file>`).
End the review comment with `<!-- {rc['marker']} -->` on its own line.
"""


def git(*args, cwd=None):
    return subprocess.run(["git", "-C", cwd or CFG["repo_dir"], *args],
                          capture_output=True, text=True, timeout=300)


def prepare_worktree(rc, unit, pr_n):
    """Create a fresh per-review worktree with the PR head checked out."""
    os.makedirs(rc["worktree_root"], exist_ok=True)
    wt = os.path.join(rc["worktree_root"], unit)
    for args in (["-C", rc["repo_dir"], "worktree", "add", "--detach", wt],
                 ["-C", wt, "fetch", "origin", f"pull/{pr_n}/head"],
                 ["-C", wt, "checkout", "--detach", "FETCH_HEAD"]):
        out = subprocess.run(["git", *args],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            subprocess.run(
                ["git", "-C", rc["repo_dir"], "worktree", "remove",
                 "--force", wt], capture_output=True, timeout=60)
            raise RuntimeError(f"{' '.join(args[1:4])}… failed: "
                               f"{out.stderr.strip()[:200]}")
    return wt


def cleanup_worktrees(rc, busy_units):
    """Drop per-review worktrees whose unit is gone (finished or dead)."""
    root = rc["worktree_root"]
    if not os.path.isdir(root):
        return
    for name in sorted(os.listdir(root)):
        wt = os.path.join(root, name)
        if not os.path.isdir(wt) or name in busy_units:
            continue
        out = subprocess.run(
            ["git", "-C", rc["repo_dir"], "worktree", "remove",
             "--force", wt], capture_output=True, text=True, timeout=120)
        if out.returncode != 0:
            log(f"worktree cleanup: could not remove {wt}: "
                f"{out.stderr.strip()[:150]}")
        else:
            log(f"worktree cleanup: removed {wt}")
    subprocess.run(["git", "-C", rc["repo_dir"], "worktree", "prune"],
                   capture_output=True, timeout=60)


def spawn_review(rc, pr, comment, follow_up=False):
    """Start a review — returns (unit/worktree name, session id or None)."""
    n, cid = pr["number"], comment["id"]
    repo_tag = f"{rc['slug']}-" if len(CFG["repos"]) > 1 else ""
    unit = (f"{rc['unit_prefix']}-{repo_tag}{n}-{cid}-"
            f"{int(datetime.now().timestamp())}")
    if DRY_RUN:
        log(f"[dry-run] would spawn {unit} for PR #{n} "
            f"(comment {cid} by @{comment.get('user', {}).get('login')})")
        return unit, None
    worktree = prepare_worktree(rc, unit, n)
    prompt = build_prompt(rc, pr, comment, worktree, follow_up)
    pfile = os.path.join(CFG["state_dir"], f"prompt-{rc['slug']}-{n}-{cid}.txt")
    with open(pfile, "w") as f:
        f.write(prompt)
    try:
        sid = daemon_spawn(rc, pr, cid, unit, worktree, prompt) \
            if USE_DAEMON else systemd_spawn(rc, n, cid, unit, worktree, pfile)
    except Exception:
        subprocess.run(
            ["git", "-C", rc["repo_dir"], "worktree", "remove",
             "--force", worktree], capture_output=True, timeout=60)
        raise
    who = f"session {sid} ({unit})" if sid else unit
    log(f"spawned {who} for PR #{n} "
        f"(comment {cid} by @{comment.get('user', {}).get('login')})")
    return unit, sid


def daemon_spawn(rc, pr, cid, unit, worktree, prompt):
    """new_session on the daemon — the prompt is delivered inline as the
    session's first message, wrapped with service provenance (`origin`)."""
    title = rc["session_title"].format(
        repo=rc["repo"], pr=pr["number"], unit=unit, cid=cid)
    return daemon_session(rc, worktree, title, prompt)


def daemon_session(rc, cwd, title, prompt):
    """new_session on the daemon — returns the session id."""
    fields = {"agent": rc["agent_profile"], "cwd": cwd,
              "title": title, "prompt": prompt,
              "origin": DAEMON_ORIGIN}
    if rc["model"]:
        fields["model"] = rc["model"]
    if rc["group"]:
        fields["group"] = rc["group"]
    try:
        res = DAEMON.call("new_session", **fields)
    except DaemonError as ex:
        if not str(ex).startswith("parse:"):
            raise
        # Older daemons may reject unknown keys — retry stripped down.
        fields.pop("group", None)
        try:
            res = DAEMON.call("new_session", **fields)
        except DaemonError as ex2:
            if not str(ex2).startswith("parse:"):
                raise
            fields.pop("origin", None)
            res = DAEMON.call("new_session", **fields)
    sid = res.get("session")
    if not sid:
        raise RuntimeError(f"new_session: unexpected reply {res!r}")
    # `group` is silently ignored by daemons predating the field — set it
    # unconditionally; harmless if new_session already applied it.
    if rc["group"]:
        try:
            DAEMON.call("set_session_group", session=sid, group=rc["group"])
        except DaemonError as ex:
            log(f"set_session_group {sid}: {ex} (session stays ungrouped)")
    return sid


def agent_argv_env(rc, worktree, fmt_extra):
    """Shared argv+env builder for direct/systemd agent execution."""
    path = ":".join([os.path.expanduser("~/.local/bin"),
                     *rc["path_extra"],
                     nvm_bin() or "/nonexistent",
                     "/usr/local/bin", "/usr/bin", "/bin"])
    fmt = {"model": rc["model"], "repo": rc["repo"] or "",
           "worktree": worktree, "pr": "", "prompt_file": ""}
    fmt.update(fmt_extra)
    argv = [a.format(**fmt) for a in rc["agent_args"]]
    env = dict(os.environ, PATH=path)
    env.update(rc["agent_env"])
    return argv, env


def systemd_spawn(rc, n, cid, unit, worktree, pfile):
    argv, env = agent_argv_env(
        rc, worktree, {"prompt_file": pfile, "pr": n})
    cmd = [
        "systemd-run", "--user",
        f"--unit={unit}",
        f"--description=AgentReviewer {rc['repo']} PR #{n} comment {cid}",
        f"--property=WorkingDirectory={worktree}",
        f"--property=RuntimeMaxSec={rc['review_timeout']}",
        "--setenv", f"PATH={env['PATH']}",
        "--collect",
    ]
    for k, v in rc["agent_env"].items():
        cmd += ["--setenv", f"{k}={v}"]
    cmd += [rc["agent_cmd"], *argv]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        raise RuntimeError(f"systemd-run failed: {out.stderr.strip()[:300]}")
    return None


def post_ack(rc, pr, comment, unit=None, queue_pos=None, duplicate=False):
    if not rc["post_acks"]:
        return
    author = comment.get("user", {}).get("login", "")
    mention = f"@{author} " if author else ""
    who = agent_desc(rc)
    if duplicate:
        body = (f"{mention}A review request is already queued or running "
                f"for this PR — ignoring this duplicate.")
    elif queue_pos is not None:
        body = (f"{mention}Queued for review (position {queue_pos}) — a "
                f"review agent ({who}) will start once a slot frees up.")
    else:
        body = (f"{mention}Launching a review — agent {who} spawned as "
                f"`{unit}`. The review will be posted as a comment on "
                f"this PR.")
    if comment.get("_kind") == "review":
        endpoint = (f"repos/{rc['repo']}/pulls/{pr['number']}/comments/"
                    f"{comment['id']}/replies")
    else:
        endpoint = f"repos/{rc['repo']}/issues/{pr['number']}/comments"
    try:
        out = subprocess.run(
            ["gh", "api", "-X", "POST", endpoint, "-f", f"body={body}"],
            capture_output=True, text=True, timeout=30,
        )
        if out.returncode != 0:
            log(f"ack post failed for comment {comment['id']}: "
                f"{out.stderr.strip()[:200]}")
        else:
            log(f"ack posted on PR #{pr['number']} for comment {comment['id']}")
    except Exception as e:
        log(f"ack post failed for comment {comment['id']}: {e}")


def ref_num(url, kind):
    m = re.search(rf"/{kind}/(\d+)$", url or "")
    return int(m.group(1)) if m else None


def daemon_setup(state):
    """Connect to the daemon if this cycle needs it. Returns the session
    metadata map ({id: {...}}) or None when unreachable."""
    global DAEMON, USE_DAEMON
    DAEMON, sessions = None, None
    want = daemon_mode_enabled() or any(
        e.get("session")
        for rs in state["repos"].values()
        for e in rs.get("comments", {}).values())
    if want:
        DAEMON = Daemon(daemon_endpoints())
        try:
            sessions = {m.get("id"): m
                        for m in DAEMON.call("list_sessions") or []}
        except DaemonError as ex:
            log(f"daemon unreachable: {ex}")
            DAEMON = None
    USE_DAEMON = daemon_mode_enabled()
    if USE_DAEMON and DAEMON is None:
        if CFG["mode"] == "daemon":
            raise RuntimeError(
                "mode=daemon: no AgentControl endpoint reachable")
        log("daemon endpoints unreachable — new spawns fall back "
            "to systemd-run this cycle")
        USE_DAEMON = False
    return sessions


def poll_repo(rc, rs, sessions, claimed_sids):
    """One poll cycle for a single repository: scan comments, recover
    dead reviews, then fill free slots from the queue."""
    def rlog(msg):
        log(f"[{rc['repo']}] {msg}")

    # An empty namespace means this repo was never polled — seed existing
    # triggers instead of reviewing the whole history at once.
    first_run = not rs
    seen = rs.setdefault("comments", {})
    if rs.get("last_check") is None:
        rs["last_check"] = datetime.now(timezone.utc).isoformat()

    prs = {p["number"]: p for p in gh_api(
        f"repos/{rc['repo']}/pulls?state=open&per_page=100")}
    since_dt = (datetime.fromisoformat(rs["last_check"])
                - timedelta(seconds=rc["since_overlap_sec"]))
    since = since_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rlog(f"scan: {len(prs)} open PR(s) since {since}")
    candidates = []
    for c in gh_api(
            f"repos/{rc['repo']}/issues/comments"
            f"?since={since}&per_page=100"):
        n = ref_num(c.get("issue_url"), "issues")
        if n in prs:
            c["_kind"], c["_pr"] = "issue", n
            candidates.append(c)
    for c in gh_api(
            f"repos/{rc['repo']}/pulls/comments"
            f"?since={since}&per_page=100"):
        n = ref_num(c.get("pull_request_url"), "pulls")
        if n in prs:
            c["_kind"], c["_pr"] = "review", n
            candidates.append(c)

    rs["last_check"] = datetime.now(timezone.utc).isoformat()

    fresh = [c for c in candidates
             if str(c["id"]) not in seen
             and rc["trigger_re"].fullmatch((c.get("body") or "").strip())]
    fresh.sort(key=lambda c: c.get("created_at", ""))

    if SEED_ONLY or first_run:
        for c in fresh:
            seen[str(c["id"])] = {"status": "seeded", "pr": c["_pr"]}
        rlog(f"seeded {len(fresh)} existing trigger comment(s), "
             "no reviews spawned")
        return

    busy_units = active_review_units(
        f"{rc['unit_prefix']}-{rc['slug']}"
        if len(CFG["repos"]) > 1 else rc["unit_prefix"])

    def reap(e):
        """Free a finished daemon session: delete it (or just kill the
        agent when keep_sessions keeps the journal). Retried on failure."""
        sid = e.get("session")
        if not sid or DAEMON is None:
            return
        if sid not in sessions:
            e.pop("session", None)
            return
        try:
            if rc["keep_sessions"]:
                DAEMON.call("kill_session", session=sid)
            else:
                DAEMON.call("delete_session", session=sid)
            sessions[sid] = {"state": "dead"}
            e.pop("session", None)
        except DaemonError as ex:
            rlog(f"reaping session {sid} failed: {ex} — retry next cycle")

    def discard(e):
        """Forget a failed session (killed or dead without verdict) —
        like reap but best-effort and skipped under keep_sessions."""
        sid = e.get("session")
        if sid and DAEMON is not None and sid in sessions \
                and not rc["keep_sessions"]:
            try:
                DAEMON.call("delete_session", session=sid)
            except DaemonError:
                pass
            sessions.pop(sid, None)

    # Recover reviews whose unit/session ended without a posted review
    # (reboot, crash, OOM). Transient units don't survive a reboot; a
    # daemon session that stays alive past review_timeout is killed the
    # same way RuntimeMaxSec used to kill the unit.
    for cid, e in seen.items():
        if e.get("status") != "spawned":
            continue
        a = entry_alive(e, sessions, busy_units)
        if a is None and not timed_out(rc, e):
            continue
        if a and e.get("session") and timed_out(rc, e) \
                and DAEMON is not None:
            try:
                DAEMON.call("kill_session", session=e["session"])
                sessions[e["session"]] = {"state": "dead"}
                discard(e)
            except DaemonError as ex:
                rlog(f"kill_session {e['session']} failed: {ex}")
                continue
            rlog(f"review for comment {cid} on PR #{e['pr_num']}: "
                 f"timed out after {rc['review_timeout']} — killed")
            a = False
        res = review_posted(rc, e)
        if res is True:
            e["status"] = "done"
            rlog(f"review for comment {cid} on PR #{e['pr_num']}: "
                 "posted — done")
            reap(e)
        elif res is False and not a:
            e["status"] = "queued"
            e["attempts"] = 0
            discard(e)
            e.pop("unit", None)
            e.pop("session", None)
            rlog(f"review for comment {cid} on PR #{e['pr_num']}: "
                 "session ended without a posted review — re-queued")

    for e in seen.values():
        if e.get("status") == "done" and e.get("session"):
            reap(e)

    queued = sorted(
        (e for e in seen.values() if e.get("status") == "queued"),
        key=lambda e: e.get("created_at") or "",
    )

    def pr_pending(n):
        for e in seen.values():
            if (e.get("pr_num") or e.get("pr")) != n:
                continue
            if e.get("status") == "queued":
                return True
            if e.get("status") == "spawned" \
                    and entry_alive(e, sessions, busy_units) is not False:
                return True
        return False

    for c in fresh:
        cid = str(c["id"])
        pr = prs[c["_pr"]]
        if pr_pending(c["_pr"]):
            seen[cid] = {"status": "ignored-duplicate", "pr_num": c["_pr"],
                         "created_at": c.get("created_at")}
            rlog(f"ignoring duplicate trigger on PR #{c['_pr']} "
                 f"(comment {cid}): a request is already queued or running")
            if not DRY_RUN:
                post_ack(rc, pr, c, duplicate=True)
            continue
        entry = {
            "status": "queued",
            "attempts": 0,
            "ack": False,
            "created_at": c.get("created_at"),
            "pr_num": c["_pr"],
            "comment": {"id": c["id"], "body": c.get("body") or "",
                        "html_url": c.get("html_url"),
                        "user": {"login": c.get("user", {}).get("login")},
                        "_kind": c["_kind"], "_pr": c["_pr"]},
            "pr": {"number": pr["number"], "title": pr.get("title"),
                   "html_url": pr.get("html_url")},
        }
        seen[cid] = entry
        queued.append(entry)
        rlog(f"queued '{rc['trigger']}' on PR #{c['_pr']} "
             f"(comment {cid} by @{c.get('user', {}).get('login')})")

    # Busy = live tracked refs + foreign units/sessions occupying slots.
    # Untracked daemon sessions only count via the service provenance —
    # a session merely parked in the sidebar group is not a review.
    # Sessions owned by another repo of this instance don't consume this
    # repo's slots; unclaimed foreign sessions conservatively count
    # against every repo.
    by_ref = {}
    busy_refs = set(busy_units)
    if sessions:
        for m in sessions.values():
            sid = m.get("id")
            if m.get("origin") != DAEMON_ORIGIN \
                    or m.get("state") in DEAD_STATES:
                continue
            owner = claimed_sids.get(sid)
            if owner is not None and owner != rc["repo"]:
                continue
            busy_refs.add(sid)
    for cid, e in seen.items():
        if e.get("status") == "spawned" and spawned_ref(e):
            by_ref[spawned_ref(e)] = (cid, e)
            if entry_alive(e, sessions, busy_units) is not False:
                busy_refs.add(spawned_ref(e))

    q_prs = len({e["pr_num"] for e in queued})
    rlog(f"queue: {len(queued)} pending review request(s) on {q_prs} PR(s), "
         f"{len(busy_refs)}/{rc['max_parallel']} slot(s) busy")
    for u in sorted(busy_refs):
        if u in by_ref:
            cid, e = by_ref[u]
            author = (e.get("comment", {}).get("user") or {}).get("login", "?")
            rlog(f"in-flight: PR #{e.get('pr_num')} comment {cid} (@{author})"
                 f" — {u}, running {fmt_age(e.get('ts'))}")
        else:
            rlog(f"in-flight: {u} (untracked)")

    def busy_worktrees():
        # Worktree dirs in use — every in-flight entry plus live foreign
        # units. Evaluated lazily: spawned-this-cycle worktrees must not
        # be reaped by the cleanup running after the spawn loop.
        wt = set(busy_units)
        wt.update(e["unit"] for e in seen.values()
                  if e.get("status") == "spawned" and e.get("unit"))
        return wt

    slots = rc["max_parallel"] - len(busy_refs)
    if slots <= 0 or not queued:
        if not DRY_RUN:
            for i, e in enumerate(queued):
                if not e.get("ack"):
                    post_ack(rc, e["pr"], e["comment"], queue_pos=i + 1)
                    e["ack"] = True
        cleanup_worktrees(rc, busy_worktrees())
        return

    for e in queued:
        if slots <= 0:
            break
        if e["pr_num"] not in prs:
            e["status"] = "skipped-closed"
            rlog(f"queued comment {e['comment']['id']} skipped: "
                 f"PR #{e['pr_num']} is no longer open")
            continue
        e["attempts"] += 1
        try:
            follow_up = any(
                x is not e
                and x.get("status") in ("spawned", "seeded", "done")
                and (x.get("pr_num") or x.get("pr")) == e["pr_num"]
                for x in seen.values())
            e["unit"], sid = spawn_review(
                rc, e["pr"], e["comment"], follow_up)
            if sid:
                e["session"] = sid
                sessions[sid] = {"id": sid, "state": "starting",
                                 "group": rc["group"]}
            e["status"] = "dry-run" if DRY_RUN else "spawned"
            e["worktree"] = os.path.join(rc["worktree_root"], e["unit"])
            if not DRY_RUN:
                post_ack(rc, e["pr"], e["comment"], unit=sid or e["unit"])
                slots -= 1
        except Exception as ex:
            if e["attempts"] >= rc["max_attempts"]:
                e["status"] = "failed"
            rlog(f"spawn failed for queued comment {e['comment']['id']} "
                 f"(attempt {e['attempts']}): {ex}")
        e["ts"] = datetime.now(timezone.utc).isoformat()

    cleanup_worktrees(rc, busy_worktrees())


def main():
    os.makedirs(CFG["state_dir"], exist_ok=True)
    lock = open(CFG["lock_file"], "w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    state = load_state()
    try:
        sessions = daemon_setup(state)
    except RuntimeError:
        save_state(state)
        raise

    # session id -> repo owning it (one state file = one owner registry,
    # so a review session only consumes slots in its own namespace).
    claimed = {}
    for name, ns in state["repos"].items():
        for e in ns.get("comments", {}).values():
            if e.get("session") and e.get("status") == "spawned":
                claimed[e["session"]] = name

    for rc in CFG["repos"]:
        try:
            poll_repo(rc, state["repos"].setdefault(rc["repo"], {}),
                      sessions, claimed)
        except Exception as ex:
            log(f"[{rc['repo']}] cycle failed: {ex}")
    save_state(state)


VERDICT_RE = re.compile(
    r"##\s*Verdict:\s*(APPROVED|CONDITIONAL|REJECTED)", re.IGNORECASE)
VERDICT_EXIT = {"APPROVED": 0, "CONDITIONAL": 4, "REJECTED": 5}
# 2 doubles as argparse's own usage-error code — bad invocation or a
# broken environment are indistinguishable to the caller anyway.
EXIT_INFRA, EXIT_NORESULT = 2, 3


def rfail(msg):
    print(f"agent-reviewer: review: {msg}", file=sys.stderr)
    sys.exit(EXIT_INFRA)


def local_review_prompt(path, base):
    return f"""You are reviewing a LOCAL git diff — there is no pull request and nothing must be posted to GitHub.

Context:
- Worktree (your working directory): {path}
- Diff under review: `{base}...HEAD`

Instructions:
1. Read the diff `git diff {base}...HEAD` (and `git log --oneline {base}..HEAD` for commit context). If the diff is empty, say so briefly and stop.
2. Analyze ONLY the changes and their direct integration with the code they touch — same rigor as a PR review: correctness, security, regressions, missing tests.
3. Write the review as your reply in this session. Do NOT post it anywhere (no `gh`, no PR comments).
4. End your review message with EXACTLY ONE verdict line:
- `## Verdict: APPROVED` — no blocking issue; the change can be merged
- `## Verdict: CONDITIONAL` — mergeable once the listed conditions are addressed
- `## Verdict: REJECTED` — blocking issues must be fixed before merge
"""


def verdict_from_runs(runs):
    """runs = message texts (contiguous agent_text runs). Returns
    (verdict|None, body): the last run carrying a Verdict line is the
    review; otherwise a concatenated tail for diagnostics."""
    for text in reversed(runs):
        m = VERDICT_RE.search(text or "")
        if m:
            return m.group(1).upper(), text
    tail = "\n\n".join(r for r in runs if r)[-4000:]
    return None, tail or "(no output captured)"


def cmd_review(args):
    """`agent-reviewer review --path WT --base REF` — review a local diff.
    Daemon backend when an endpoint is reachable (auto/daemon mode),
    direct foreground agent execution otherwise (and in mode=systemd)."""
    global CFG, DAEMON
    CFG = load_config(args.config, allow_missing=True)
    path = expand(args.path)
    base = args.base
    if not os.path.isdir(path):
        rfail(f"no such directory: {path}")
    for chk, what in (
            (["rev-parse", "--git-dir"], "not a git worktree"),
            (["rev-parse", "--verify", "--quiet", f"{base}^{{commit}}"],
             f"unknown ref '{base}'")):
        out = subprocess.run(["git", "-C", path, *chk],
                             capture_output=True, text=True, timeout=30)
        if out.returncode != 0:
            rfail(f"{path}: {what}")

    # Agent/daemon defaults: primary repo config when present, globals
    # otherwise — the local worktree needs no repo_dir/worktree_root.
    rc = dict(CFG["repos"][0]) if CFG["repos"] else dict(CFG)
    rc.setdefault("slug", "local")
    if args.timeout:
        rc["review_timeout_sec"] = args.timeout
        rc["review_timeout"] = f"{args.timeout}s"
    timeout = rc.get("review_timeout_sec") or 10800
    prompt = local_review_prompt(path, base)
    title = (f"local review {os.path.basename(path.rstrip('/'))} "
             f"({base}...HEAD)")
    log(f"local review: {path} {base}...HEAD (timeout "
        f"{rc['review_timeout']})")

    DAEMON = None
    if CFG["mode"] != "systemd":
        DAEMON = Daemon(daemon_endpoints())
        try:
            DAEMON.call("list_sessions")
        except DaemonError:
            DAEMON = None
        if DAEMON is None and CFG["mode"] == "daemon":
            rfail("mode=daemon but no AgentControl endpoint is "
                  "reachable")
    if DAEMON is not None:
        rc["repo"] = rc.get("repo") or "local"
        sid = daemon_session(rc, path, title, prompt)
        log(f"local review session {sid} spawned")
        if args.detach:
            print(sid)
            return 0
        verdict, body = wait_daemon_review(rc, sid, timeout)
    else:
        if args.detach:
            rfail("--detach requires a reachable daemon endpoint")
        if CFG["mode"] == "systemd":
            log("mode=systemd — running the agent in the foreground")
        else:
            log("no daemon endpoint — running the agent in the "
                "foreground")
        verdict, body = run_direct_review(rc, path, prompt, timeout)

    if args.out:
        with open(args.out, "w") as f:
            f.write(body.rstrip() + "\n")
        log(f"review written to {args.out}")
    print("\n" + body.rstrip())
    code = VERDICT_EXIT.get(verdict, EXIT_NORESULT)
    log(f"local review: verdict={verdict or 'none'} → exit {code}")
    return code


def wait_daemon_review(rc, sid, timeout):
    """Poll the session until a Verdict line appears in its events, the
    session dies, or the timeout hits. Reaps like a PR review.
    agent_text arrives in streamed chunks — group contiguous runs into
    whole messages so a chunked review is not truncated."""
    deadline = time.time() + timeout
    runs, cur, after, dead = [], [], 0, False
    has_verdict = False

    def flush():
        if cur:
            runs.append("".join(cur))
            cur.clear()

    while time.time() < deadline and not has_verdict:
        try:
            ev = DAEMON.call("events", session=sid,
                             after_seq=after, limit=200)
            for e in ev.get("events", []):
                after = max(after, int(e.get("seq") or 0))
                if e.get("type") == "agent_text" and e.get("text"):
                    cur.append(e["text"])
                    if VERDICT_RE.search("".join(cur)):
                        has_verdict = True
                else:
                    flush()
        except DaemonError as ex:
            log(f"events {sid}: {ex} — retrying")
        if has_verdict:
            break
        try:
            info = DAEMON.call("session_info", session=sid)
            state = ((info or {}).get("meta") or {}).get("state", "dead")
        except DaemonError:
            state = "dead"
        # A one-shot review is over when the session dies — or when the
        # agent ends its turn and parks idle/waiting_input having already
        # produced output (there is no GitHub oracle here: silence is the
        # signal). An idle session with no events yet is still ramping up.
        if state in DEAD_STATES:
            dead = True
            break
        if state in ("idle", "waiting_input") and (cur or runs):
            break
        time.sleep(5)
    flush()
    timed = time.time() >= deadline and not dead and not has_verdict
    if timed:
        log(f"local review timed out after {rc['review_timeout']} — "
            f"killing {sid}")
        try:
            DAEMON.call("kill_session", session=sid)
        except DaemonError as ex:
            log(f"kill_session {sid}: {ex}")
    try:
        if rc["keep_sessions"]:
            if not dead and not timed:
                DAEMON.call("kill_session", session=sid)
        else:
            DAEMON.call("delete_session", session=sid)
    except DaemonError as ex:
        log(f"session cleanup {sid}: {ex}")
    return verdict_from_runs(runs)


def run_direct_review(rc, path, prompt, timeout):
    """Fallback backend: run the configured agent in the worktree and
    parse its stdout for the Verdict line."""
    fd, pfile = tempfile.mkstemp(
        dir=CFG["state_dir"] if os.path.isdir(CFG["state_dir"]) else None,
        prefix="prompt-local-", suffix=".txt", text=True)
    with os.fdopen(fd, "w") as f:
        f.write(prompt)
    argv, env = agent_argv_env(rc, path, {"prompt_file": pfile})
    try:
        out = subprocess.run(
            [rc["agent_cmd"], *argv], cwd=path, env=env,
            capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as ex:
        tail = (ex.stdout or "")[-4000:] if isinstance(
            ex.stdout, str) else ""
        return None, (f"(local review timed out — no verdict)\n{tail}"
                      .rstrip())
    finally:
        os.unlink(pfile)
    if out.returncode != 0 and not VERDICT_RE.search(out.stdout):
        return None, (f"(agent exited {out.returncode})\n"
                      f"{out.stderr.strip()[-2000:]}")
    return verdict_from_runs([out.stdout])


def cli():
    global DRY_RUN, SEED_ONLY, CFG
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--version", action="version",
                    version=f"%(prog)s {__version__}")
    ap.add_argument("--config", default=os.environ.get(
        "AGENTREVIEWER_CONFIG", expand(DEFAULT_CONFIG)))
    sub = ap.add_subparsers(dest="cmd")
    rp = sub.add_parser(
        "review", help="review a local diff BASE...HEAD in a worktree "
                       "(no PR required)")
    rp.add_argument("--path", required=True, metavar="DIR",
                    help="existing git worktree to review")
    rp.add_argument("--base", required=True, metavar="REF",
                    help="base ref, e.g. origin/next_release")
    rp.add_argument("--out", metavar="FILE",
                    help="also write the review body to FILE")
    rp.add_argument("--timeout", type=int, metavar="SEC",
                    help="override review_timeout")
    rp.add_argument("--detach", action="store_true",
                    help="daemon mode: print the session id and exit")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seed", action="store_true")
    ap.add_argument("--watch", action="store_true",
                    help="loop poll cycles in-process (managed-component "
                         "mode; the daemon restarts on failure)")
    ap.add_argument("--interval", type=int, metavar="SEC",
                    default=int(os.environ.get(
                        "AGENTREVIEWER_INTERVAL", "60")),
                    help="seconds between cycles in --watch mode")
    args = ap.parse_args()
    if args.cmd == "review":
        sys.exit(cmd_review(args))

    DRY_RUN = args.dry_run
    SEED_ONLY = args.seed
    CFG = load_config(args.config)
    if not CFG["repos"]:
        sys.exit(f"agent-reviewer: config '{args.config}': "
                 "no repository configured (set repo/repo_dir or repos[])")

    while True:
        try:
            main()
        except BlockingIOError:
            log("another instance is running, skipping")
        except Exception as e:
            log(f"error: {e}")
            if not args.watch:
                sys.exit(1)
        if not args.watch:
            break
        time.sleep(max(5, args.interval))


if __name__ == "__main__":
    cli()
