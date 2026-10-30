#!/usr/bin/env python3
"""AgentReviewer — watch GitHub PRs for review-trigger comments.

Each new bare trigger comment (e.g. `/agent review`) on an open PR is
queued; up to `max_parallel` agent sessions run concurrently, each in its
own git worktree with the PR head checked out. The agent posts the review
back as a PR comment.

Designed to run every minute from agentreviewer-watch.timer (systemd --user).
Everything is configured via a TOML file — see config.example.toml.

Usage:
  agent-reviewer [--config PATH]   one poll cycle (used by the systemd unit)
  agent-reviewer --config PATH --dry-run
                                   detect pending comments, build prompts,
                                   create no worktree, spawn nothing
  agent-reviewer --config PATH --seed
                                   mark all current matching comments as
                                   processed, spawn nothing
"""

import argparse
import fcntl
import glob
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from datetime import datetime, timedelta, timezone

DEFAULT_CONFIG = "~/.config/agentreviewer/config.toml"
DEFAULT_AGENT_ARGS = [
    "--model", "{model}",
    "--permission-mode", "dangerous",
    "--respect-workspace-trust", "false",
    "--prompt-file", "{prompt_file}",
    "--print",
]


def expand(p):
    return os.path.abspath(os.path.expandvars(os.path.expanduser(p)))


def load_config(path):
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    def req(key):
        if not raw.get(key):
            sys.exit(f"agent-reviewer: config '{path}': missing required '{key}'")
        return raw[key]

    agent = raw.get("agent", {})
    cfg = {
        "repo": req("repo"),
        "repo_dir": expand(req("repo_dir")),
        "worktree_root": expand(req("worktree_root")),
        "trigger": str(raw.get("trigger", "/agent review")),
        "max_parallel": max(1, int(raw.get("max_parallel", 1))),
        "review_timeout": str(raw.get("review_timeout", "3h")),
        "max_attempts": int(raw.get("max_attempts", 5)),
        "since_overlap_sec": int(raw.get("since_overlap_sec", 600)),
        "unit_prefix": re.sub(r"[^a-zA-Z0-9_.-]", "-",
                              str(raw.get("unit_prefix", "agentreviewer"))),
        "marker": str(raw.get("marker", "agentreviewer")),
        "post_acks": bool(raw.get("post_acks", True)),
        "agent_cmd": str(agent.get("command", "devin")),
        "agent_args": [str(a) for a in agent.get("args", DEFAULT_AGENT_ARGS)],
        "model": str(agent.get("model", "swe-2-max")),
        "agent_env": {str(k): str(v) for k, v in agent.get("env", {}).items()},
        "path_extra": [expand(p) for p in agent.get("path_extra", [])],
        "state_dir": expand(os.environ.get(
            "AGENTREVIEWER_STATE_DIR",
            raw.get("state_dir", "~/.local/state/agentreviewer"))),
    }
    trig = r"\s+".join(re.escape(t) for t in cfg["trigger"].split())
    cfg["trigger_re"] = re.compile(trig + r"\s*", re.IGNORECASE)
    cfg["state_file"] = os.path.join(cfg["state_dir"], "state.json")
    cfg["log_file"] = os.path.join(cfg["state_dir"], "watcher.log")
    cfg["lock_file"] = os.path.join(cfg["state_dir"], "lock")
    return cfg


CFG = None  # set in __main__


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
    try:
        with open(CFG["state_file"]) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"last_check": None, "comments": {}}


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


def agent_desc():
    d = CFG["agent_cmd"].rsplit("/", 1)[-1]
    return f"{d} ({CFG['model']})" if CFG["model"] else d


def active_review_units():
    out = subprocess.run(
        ["systemctl", "--user", "list-units", "--type=service",
         "--state=active,activating", "--no-legend", "--plain",
         f"{CFG['unit_prefix']}-*"],
        capture_output=True, text=True, timeout=30,
    )
    if out.returncode not in (0, 1):
        return set()
    return {l.split()[0].removesuffix(".service")
            for l in out.stdout.splitlines() if l.strip()}


def review_posted(entry):
    """True: review landed on the PR. False: none found. None: check failed."""
    try:
        comments = gh_api(
            f"repos/{CFG['repo']}/issues/{entry['pr_num']}"
            "/comments?per_page=100")
    except Exception as ex:
        log(f"review check failed for comment "
            f"{entry.get('comment', {}).get('id')} "
            f"(PR #{entry.get('pr_num')}): {ex}")
        return None
    trig = entry.get("created_at") or ""
    marker = f"<!-- {CFG['marker']} -->"
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


def build_prompt(pr, comment, worktree, follow_up=False):
    repo = CFG["repo"]
    body = (comment.get("body") or "").strip()
    n = pr["number"]
    header = f"""A user commented `{CFG['trigger']}` on a pull request on GitHub. The full event details are included below.

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
3. This is a FOLLOW-UP review: this PR was already reviewed at least once. Find the previous review comment(s) on the PR posted in response to earlier `{CFG['trigger']}` commands (they start with `## Verdict:` — older ones may use a `## Review` heading instead) and identify the commits pushed since the last one (`gh api repos/{repo}/pulls/{n}/commits` lists head SHAs and dates). If no previous review comment exists on the PR, review the full diff instead.
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
End the review comment with `<!-- {CFG['marker']} -->` on its own line.
"""


def git(*args, cwd=None):
    return subprocess.run(["git", "-C", cwd or CFG["repo_dir"], *args],
                          capture_output=True, text=True, timeout=300)


def prepare_worktree(unit, pr_n):
    """Create a fresh per-review worktree with the PR head checked out."""
    os.makedirs(CFG["worktree_root"], exist_ok=True)
    wt = os.path.join(CFG["worktree_root"], unit)
    for args in (["-C", CFG["repo_dir"], "worktree", "add", "--detach", wt],
                 ["-C", wt, "fetch", "origin", f"pull/{pr_n}/head"],
                 ["-C", wt, "checkout", "--detach", "FETCH_HEAD"]):
        out = subprocess.run(["git", *args],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            subprocess.run(
                ["git", "-C", CFG["repo_dir"], "worktree", "remove",
                 "--force", wt], capture_output=True, timeout=60)
            raise RuntimeError(f"{' '.join(args[1:4])}… failed: "
                               f"{out.stderr.strip()[:200]}")
    return wt


def cleanup_worktrees(busy_units):
    """Drop per-review worktrees whose unit is gone (finished or dead)."""
    root = CFG["worktree_root"]
    if not os.path.isdir(root):
        return
    for name in sorted(os.listdir(root)):
        wt = os.path.join(root, name)
        if not os.path.isdir(wt) or name in busy_units:
            continue
        out = subprocess.run(
            ["git", "-C", CFG["repo_dir"], "worktree", "remove",
             "--force", wt], capture_output=True, text=True, timeout=120)
        if out.returncode != 0:
            log(f"worktree cleanup: could not remove {wt}: "
                f"{out.stderr.strip()[:150]}")
        else:
            log(f"worktree cleanup: removed {wt}")
    subprocess.run(["git", "-C", CFG["repo_dir"], "worktree", "prune"],
                   capture_output=True, timeout=60)


def spawn_review(pr, comment, follow_up=False):
    n, cid = pr["number"], comment["id"]
    unit = f"{CFG['unit_prefix']}-{n}-{cid}-{int(datetime.now().timestamp())}"
    if DRY_RUN:
        log(f"[dry-run] would spawn {unit} for PR #{n} "
            f"(comment {cid} by @{comment.get('user', {}).get('login')})")
        return unit
    worktree = prepare_worktree(unit, n)
    prompt = build_prompt(pr, comment, worktree, follow_up)
    pfile = os.path.join(CFG["state_dir"], f"prompt-{n}-{cid}.txt")
    with open(pfile, "w") as f:
        f.write(prompt)
    path = ":".join([os.path.expanduser("~/.local/bin"),
                     *CFG["path_extra"],
                     nvm_bin() or "/nonexistent",
                     "/usr/local/bin", "/usr/bin", "/bin"])
    fmt = {"prompt_file": pfile, "model": CFG["model"],
           "repo": CFG["repo"], "pr": n, "worktree": worktree}
    agent_argv = [a.format(**fmt) for a in CFG["agent_args"]]
    cmd = [
        "systemd-run", "--user",
        f"--unit={unit}",
        f"--description=AgentReviewer {CFG['repo']} PR #{n} comment {cid}",
        f"--property=WorkingDirectory={worktree}",
        f"--property=RuntimeMaxSec={CFG['review_timeout']}",
        "--setenv", f"PATH={path}",
        "--collect",
    ]
    for k, v in CFG["agent_env"].items():
        cmd += ["--setenv", f"{k}={v}"]
    cmd += [CFG["agent_cmd"], *agent_argv]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        subprocess.run(
            ["git", "-C", CFG["repo_dir"], "worktree", "remove",
             "--force", worktree], capture_output=True, timeout=60)
        raise RuntimeError(f"systemd-run failed: {out.stderr.strip()[:300]}")
    log(f"spawned {unit} for PR #{n} "
        f"(comment {cid} by @{comment.get('user', {}).get('login')})")
    return unit


def post_ack(pr, comment, unit=None, queue_pos=None, duplicate=False):
    if not CFG["post_acks"]:
        return
    author = comment.get("user", {}).get("login", "")
    mention = f"@{author} " if author else ""
    who = agent_desc()
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
        endpoint = (f"repos/{CFG['repo']}/pulls/{pr['number']}/comments/"
                    f"{comment['id']}/replies")
    else:
        endpoint = f"repos/{CFG['repo']}/issues/{pr['number']}/comments"
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


def main():
    os.makedirs(CFG["state_dir"], exist_ok=True)
    lock = open(CFG["lock_file"], "w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    state = load_state()
    if state["last_check"] is None:
        state["last_check"] = datetime.now(timezone.utc).isoformat()

    prs = {p["number"]: p for p in gh_api(
        f"repos/{CFG['repo']}/pulls?state=open&per_page=100")}
    since_dt = (datetime.fromisoformat(state["last_check"])
                - timedelta(seconds=CFG["since_overlap_sec"]))
    since = since_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log(f"scan: {len(prs)} open PR(s) since {since}")
    candidates = []
    for c in gh_api(
            f"repos/{CFG['repo']}/issues/comments"
            f"?since={since}&per_page=100"):
        n = ref_num(c.get("issue_url"), "issues")
        if n in prs:
            c["_kind"], c["_pr"] = "issue", n
            candidates.append(c)
    for c in gh_api(
            f"repos/{CFG['repo']}/pulls/comments"
            f"?since={since}&per_page=100"):
        n = ref_num(c.get("pull_request_url"), "pulls")
        if n in prs:
            c["_kind"], c["_pr"] = "review", n
            candidates.append(c)

    state["last_check"] = datetime.now(timezone.utc).isoformat()

    seen = state["comments"]
    fresh = [c for c in candidates
             if str(c["id"]) not in seen
             and CFG["trigger_re"].fullmatch((c.get("body") or "").strip())]
    fresh.sort(key=lambda c: c.get("created_at", ""))

    if SEED_ONLY or not os.path.exists(CFG["state_file"]):
        for c in fresh:
            seen[str(c["id"])] = {"status": "seeded", "pr": c["_pr"]}
        save_state(state)
        log(f"seeded {len(fresh)} existing trigger comment(s), "
            "no reviews spawned")
        return

    busy_units = active_review_units()

    # Recover reviews whose unit vanished without a posted review
    # (reboot, crash, OOM). Transient units don't survive a reboot.
    for cid, e in seen.items():
        if e.get("status") == "spawned" and e.get("unit") not in busy_units:
            res = review_posted(e)
            if res is True:
                e["status"] = "done"
                log(f"review for comment {cid} on PR #{e['pr_num']}: "
                    "posted — done")
            elif res is False:
                e["status"] = "queued"
                e["attempts"] = 0
                e.pop("unit", None)
                log(f"review for comment {cid} on PR #{e['pr_num']}: unit "
                    "ended without a posted review — re-queued")

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
            if e.get("status") == "spawned" and e.get("unit") in busy_units:
                return True
        return False

    for c in fresh:
        cid = str(c["id"])
        pr = prs[c["_pr"]]
        if pr_pending(c["_pr"]):
            seen[cid] = {"status": "ignored-duplicate", "pr_num": c["_pr"],
                         "created_at": c.get("created_at")}
            log(f"ignoring duplicate trigger on PR #{c['_pr']} "
                f"(comment {cid}): a request is already queued or running")
            if not DRY_RUN:
                post_ack(pr, c, duplicate=True)
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
        log(f"queued '{CFG['trigger']}' on PR #{c['_pr']} "
            f"(comment {cid} by @{c.get('user', {}).get('login')})")

    q_prs = len({e["pr_num"] for e in queued})
    log(f"queue: {len(queued)} pending review request(s) on {q_prs} PR(s), "
        f"{len(busy_units)}/{CFG['max_parallel']} slot(s) busy")
    by_unit = {e.get("unit"): (cid, e) for cid, e in seen.items()}
    for u in sorted(busy_units):
        if u in by_unit:
            cid, e = by_unit[u]
            author = (e.get("comment", {}).get("user") or {}).get("login", "?")
            log(f"in-flight: PR #{e.get('pr_num')} comment {cid} (@{author})"
                f" — {u}, running {fmt_age(e.get('ts'))}")
        else:
            log(f"in-flight: {u} (untracked)")

    slots = CFG["max_parallel"] - len(busy_units)
    if slots <= 0 or not queued:
        if not DRY_RUN:
            for i, e in enumerate(queued):
                if not e.get("ack"):
                    post_ack(e["pr"], e["comment"], queue_pos=i + 1)
                    e["ack"] = True
        cleanup_worktrees(busy_units)
        save_state(state)
        return

    for e in queued:
        if slots <= 0:
            break
        if e["pr_num"] not in prs:
            e["status"] = "skipped-closed"
            log(f"queued comment {e['comment']['id']} skipped: "
                f"PR #{e['pr_num']} is no longer open")
            continue
        e["attempts"] += 1
        try:
            follow_up = any(
                x is not e
                and x.get("status") in ("spawned", "seeded", "done")
                and (x.get("pr_num") or x.get("pr")) == e["pr_num"]
                for x in seen.values())
            e["unit"] = spawn_review(e["pr"], e["comment"], follow_up)
            e["status"] = "dry-run" if DRY_RUN else "spawned"
            e["worktree"] = os.path.join(CFG["worktree_root"], e["unit"])
            if not DRY_RUN:
                post_ack(e["pr"], e["comment"], unit=e["unit"])
                slots -= 1
        except Exception as ex:
            if e["attempts"] >= CFG["max_attempts"]:
                e["status"] = "failed"
            log(f"spawn failed for queued comment {e['comment']['id']} "
                f"(attempt {e['attempts']}): {ex}")
        e["ts"] = datetime.now(timezone.utc).isoformat()

    cleanup_worktrees(busy_units)
    save_state(state)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--config", default=os.environ.get(
        "AGENTREVIEWER_CONFIG", expand(DEFAULT_CONFIG)))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--seed", action="store_true")
    args = ap.parse_args()

    DRY_RUN = args.dry_run
    SEED_ONLY = args.seed
    CFG = load_config(args.config)

    try:
        main()
    except fcntl.error:
        log("another instance is running, skipping")
    except Exception as e:
        log(f"error: {e}")
        sys.exit(1)
