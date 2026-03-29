import os
import json
import logging
import requests
import anthropic
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

client = anthropic.Anthropic()

# ── Prompts ───────────────────────────────────────────────────────────────────

SUMMARY_SYSTEM_PROMPT = """You are a senior data engineer reviewing a pull request
on an equity analytics pipeline. The pipeline ingests S&P 500 prices, FRED macro
indicators, and fundamental financial data into Snowflake, transforms them with dbt,
and exposes them through a Streamlit + Claude chat application.

You will be given the git diff for this PR. Your job is to write a concise,
plain-English summary of what the PR does — what changed, why it matters, and
any operational impact (e.g. requires a dbt full-refresh, changes pipeline schedule,
adds new tables, modifies existing tests).

Format your response as:

## PR Summary

**What changed**
A 2–4 sentence description of the changes in plain English. Focus on intent and
impact, not line-by-line description of the diff.

**Files changed**
A brief list of the files modified and what role each plays.

**Operational impact**
Any actions required after merge: dbt full-refresh, Prefect redeployment,
Snowflake schema changes, environment variable additions, etc.
Write "None" if no post-merge action is needed.

**Risk level**
One of: Low / Medium / High — with a one-line justification.

Be direct and specific. Do not pad. If the diff is too large to summarise fully,
focus on the most significant changes."""

REVIEW_SYSTEM_PROMPT = """You are a senior data engineer with 10+ years of experience at 
hedge funds and financial services firms. You have deep expertise in:
- Python data engineering best practices
- Snowflake and SQL optimization
- dbt modeling patterns
- Pipeline reliability and observability
- Security and credential management
- Financial data quality and lineage

Review the provided code files and identify:

1. SECURITY ISSUES — hardcoded credentials, exposed secrets, insufficient access controls
2. DATA ENGINEERING ANTI-PATTERNS — overwrite loads where incremental is appropriate, 
   missing error handling, no idempotency, schema drift risks
3. SQL ISSUES — inefficient queries, missing filters, type casting problems, 
   window function misuse
4. PRODUCTION READINESS GAPS — missing retries, no logging, poor observability, 
   no alerting
5. dbt BEST PRACTICE VIOLATIONS — missing tests, undeclared sources, hardcoded 
   schema names, missing grain declarations
6. FINANCIAL DATA CONCERNS — lineage gaps, audit trail issues, data quality risks 
   specific to financial services
7. CODE QUALITY — poor naming, missing docstrings, overly complex functions, 
   lack of type hints

Format your response as:

## Critical Issues
Issues that would cause failures or security breaches in production.

## Warnings  
Issues that reduce reliability or maintainability.

## Suggestions
Nice-to-have improvements that would elevate code quality.

## Strengths
What's done well — be specific.

For each issue include: file name, specific line or section, explanation, 
and the correct pattern. Be direct and specific. No padding."""

# ── Config ────────────────────────────────────────────────────────────────────

EXTENSIONS_TO_REVIEW = {'.py', '.sql', '.yml', '.yaml'}

SKIP_DIRS = {
    'target', 'dbt_packages', '__pycache__',
    '.git', 'node_modules', '.github', 'logs'
}

SKIP_FILES = {
    'deploy.py', 'test_conn.py', 'code_reviewer.py'
}

SKIP_PATHS = {
    'dbt_project.yml'
}

MAX_CHUNK_CHARS    = 80_000
MAX_SINGLE_FILE_CHARS = 60_000
MAX_DIFF_CHARS     = 40_000   # diff sent to summary prompt; truncated if enormous


# ── GitHub diff fetching ──────────────────────────────────────────────────────

def get_pr_number() -> int | None:
    """
    Read the PR number from the GitHub Actions event payload.
    Returns None if not running in a GitHub Actions PR context.
    """
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        logger.info("GITHUB_EVENT_PATH not set — not running in GitHub Actions")
        return None
    try:
        with open(event_path, encoding="utf-8") as f:
            event = json.load(f)
        pr_number = event.get("pull_request", {}).get("number")
        if pr_number:
            logger.info("PR number: %d", pr_number)
        return pr_number
    except Exception as e:
        logger.warning("Could not read event payload: %s", e)
        return None


def fetch_pr_diff(pr_number: int) -> str | None:
    """
    Fetch the unified diff for a PR from the GitHub API.

    Uses GITHUB_TOKEN (automatically available in Actions) and
    GITHUB_REPOSITORY (e.g. 'NanoCarbon/equityanalytics').

    Returns the raw diff string, or None on failure.
    """
    repo      = os.environ.get("GITHUB_REPOSITORY")
    token     = os.environ.get("GITHUB_TOKEN")

    if not repo or not token:
        logger.warning("GITHUB_REPOSITORY or GITHUB_TOKEN not set — skipping diff fetch")
        return None

    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3.diff",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        diff = response.text
        logger.info("Fetched diff: %d chars", len(diff))
        return diff
    except requests.HTTPError as e:
        logger.warning("GitHub API error fetching diff: %s", e)
        return None
    except Exception as e:
        logger.warning("Unexpected error fetching diff: %s", e)
        return None


def fetch_pr_metadata(pr_number: int) -> dict:
    """
    Fetch PR title, description, and changed file list from the GitHub API.
    Returns an empty dict on failure.
    """
    repo  = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")

    if not repo or not token:
        return {}

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        pr_url    = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
        files_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"

        pr_resp    = requests.get(pr_url,    headers=headers, timeout=30)
        files_resp = requests.get(files_url, headers=headers, timeout=30)
        pr_resp.raise_for_status()
        files_resp.raise_for_status()

        pr_data    = pr_resp.json()
        files_data = files_resp.json()

        return {
            "title":       pr_data.get("title", ""),
            "description": pr_data.get("body", "") or "",
            "files": [
                {
                    "filename": f["filename"],
                    "status":   f["status"],        # added / modified / removed
                    "additions": f["additions"],
                    "deletions": f["deletions"],
                }
                for f in files_data
            ],
        }
    except Exception as e:
        logger.warning("Could not fetch PR metadata: %s", e)
        return {}


# ── PR summary generation ─────────────────────────────────────────────────────

def generate_pr_summary(diff: str, metadata: dict) -> str:
    """
    Send the PR diff and metadata to Claude and return a plain-English summary
    of what the PR does, its operational impact, and risk level.
    """
    # Truncate very large diffs — focus on the signal, not every whitespace change
    if len(diff) > MAX_DIFF_CHARS:
        logger.warning("Diff is %d chars — truncating to %d for summary", len(diff), MAX_DIFF_CHARS)
        diff = diff[:MAX_DIFF_CHARS] + "\n\n... [DIFF TRUNCATED]"

    # Build context block from metadata if available
    context = ""
    if metadata:
        context += f"**PR title:** {metadata.get('title', 'N/A')}\n"
        description = metadata.get("description", "").strip()
        if description:
            context += f"**PR description:** {description}\n"
        files = metadata.get("files", [])
        if files:
            context += f"\n**Changed files ({len(files)}):**\n"
            for f in files:
                context += (
                    f"  - `{f['filename']}` "
                    f"({f['status']}, +{f['additions']} -{f['deletions']})\n"
                )
        context += "\n"

    user_content = f"{context}**Diff:**\n```diff\n{diff}\n```"

    logger.info("Generating PR summary (~%d chars to Claude)", len(user_content))

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        system=SUMMARY_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
        timeout=60,
    )
    return response.content[0].text


# ── File collection and review ────────────────────────────────────────────────

def collect_files(repo_root: str) -> dict:
    """Walk the repo and collect reviewable files."""
    files = {}
    root = Path(repo_root)

    for path in root.rglob('*'):
        if any(skip in path.parts for skip in SKIP_DIRS):
            continue
        if path.suffix not in EXTENSIONS_TO_REVIEW:
            continue
        if path.name in SKIP_FILES:
            continue
        if str(path.relative_to(root)) in SKIP_PATHS:
            continue
        if not path.is_file():
            continue
        if path.stat().st_size == 0:
            continue

        relative = str(path.relative_to(root))
        try:
            content = path.read_text(encoding='utf-8')
            if len(content) > MAX_SINGLE_FILE_CHARS:
                logger.warning(
                    "%s is %d chars — truncating to %d for review",
                    relative, len(content), MAX_SINGLE_FILE_CHARS
                )
                content = content[:MAX_SINGLE_FILE_CHARS] + "\n\n... [TRUNCATED]"
            files[relative] = content
        except Exception as e:
            logger.warning("Could not read %s: %s", relative, e)

    return files


def chunk_files(files: dict, max_chars: int = MAX_CHUNK_CHARS) -> list[dict]:
    """Split files into chunks to stay within API context limits."""
    chunks = []
    current_chunk: dict = {}
    current_size = 0

    for filepath, content in files.items():
        file_size = len(content)
        if current_size + file_size > max_chars and current_chunk:
            chunks.append(current_chunk)
            current_chunk = {}
            current_size = 0
        current_chunk[filepath] = content
        current_size += file_size

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def review_chunk(files: dict, chunk_num: int, total_chunks: int) -> str:
    """Send a chunk of files to Claude for code review."""
    content = f"Code review request (batch {chunk_num} of {total_chunks}).\n\n"
    content += "Review these files from an equity analytics data pipeline:\n\n"
    for filepath, code in files.items():
        content += f"=== {filepath} ===\n```\n{code}\n```\n\n"

    logger.info(
        "Sending batch %d/%d to Claude (%d files, ~%d chars)",
        chunk_num, total_chunks, len(files), len(content)
    )

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=REVIEW_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}],
        timeout=120,
    )
    return response.content[0].text


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    repo_root  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    generated  = datetime.now().strftime('%Y-%m-%d %H:%M')
    sections   = []

    # ── 1. PR summary from diff ────────────────────────────────────────────────
    pr_number = get_pr_number()
    pr_summary = None

    if pr_number:
        logger.info("Fetching diff and metadata for PR #%d", pr_number)
        diff     = fetch_pr_diff(pr_number)
        metadata = fetch_pr_metadata(pr_number)

        if diff:
            logger.info("Generating PR summary...")
            pr_summary = generate_pr_summary(diff, metadata)
            sections.append(pr_summary)
        else:
            logger.warning("No diff available — skipping PR summary")
            sections.append("## PR Summary\n\n_Diff not available — summary skipped._")
    else:
        logger.info("Not in a PR context — skipping PR summary")
        sections.append("## PR Summary\n\n_Running outside GitHub Actions PR context — summary skipped._")

    # ── 2. File-by-file code review ────────────────────────────────────────────
    logger.info("Collecting files for review from %s", repo_root)
    files = collect_files(repo_root)

    logger.info("Found %d files to review:", len(files))
    for f in sorted(files.keys()):
        logger.info("  %s", f)

    chunks = chunk_files(files)
    logger.info("Split into %d review batch(es)", len(chunks))

    all_reviews = []
    for i, chunk in enumerate(chunks, 1):
        logger.info("Reviewing batch %d/%d: %s", i, len(chunks), list(chunk.keys()))
        review = review_chunk(chunk, i, len(chunks))
        all_reviews.append(f"### Batch {i} of {len(chunks)}\n\n{review}")

    sections.append("---\n\n# Code Review\n\n" + "\n\n---\n\n".join(all_reviews))

    # ── 3. Write output ────────────────────────────────────────────────────────
    full_output = (
        f"# Automated Code Review\n\n"
        f"Generated: {generated}"
        + (f" · PR #{pr_number}" if pr_number else "")
        + "\n\n---\n\n"
        + "\n\n".join(sections)
    )

    output_path = Path(repo_root) / "code_review.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_output)

    logger.info("Review saved to %s", output_path)
    print("\n" + "=" * 60)
    print(full_output)


if __name__ == "__main__":
    main()