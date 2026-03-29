import os
import logging
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

SYSTEM_PROMPT = """You are a senior data engineer with 10+ years of experience at 
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

EXTENSIONS_TO_REVIEW  = {'.py', '.sql', '.yml', '.yaml'}
SKIP_DIRS             = {'target', 'dbt_packages', '__pycache__', '.git', 'node_modules', '.github', 'logs'}
SKIP_FILES            = {'deploy.py', 'test_conn.py', 'code_reviewer.py'}
SKIP_PATHS            = {'dbt_project.yml'}

MAX_CHUNK_CHARS       = 80_000
MAX_SINGLE_FILE_CHARS = 60_000


def collect_files(repo_root: str) -> dict:
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
        if not path.is_file() or path.stat().st_size == 0:
            continue

        relative = str(path.relative_to(root))
        try:
            content = path.read_text(encoding='utf-8')
            if len(content) > MAX_SINGLE_FILE_CHARS:
                logger.warning("%s is %d chars — truncating to %d", relative, len(content), MAX_SINGLE_FILE_CHARS)
                content = content[:MAX_SINGLE_FILE_CHARS] + "\n\n... [TRUNCATED]"
            files[relative] = content
        except Exception as e:
            logger.warning("Could not read %s: %s", relative, e)

    return files


def chunk_files(files: dict, max_chars: int = MAX_CHUNK_CHARS) -> list[dict]:
    chunks: list[dict] = []
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
    content = f"Code review request (batch {chunk_num} of {total_chunks}).\n\n"
    content += "Review these files from an equity analytics data pipeline:\n\n"
    for filepath, code in files.items():
        content += f"=== {filepath} ===\n```\n{code}\n```\n\n"

    logger.info("Sending batch %d/%d to Claude (%d files, ~%d chars)",
                chunk_num, total_chunks, len(files), len(content))

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}],
        timeout=120,
    )
    return response.content[0].text


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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

    full_output = (
        f"# Automated Code Review\n\n"
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"---\n\n"
        + "\n\n---\n\n".join(all_reviews)
    )

    output_path = Path(repo_root) / "code_review.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_output)

    logger.info("Review saved to %s", output_path)
    print("\n" + "=" * 60)
    print(full_output)


if __name__ == "__main__":
    main()