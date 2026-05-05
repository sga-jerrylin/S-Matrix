"""
Run golden natural-language query cases against the API.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from golden_runner import build_headers, run_cases, summarize_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:38018")
    parser.add_argument("--api-key", default="")
    parser.add_argument(
        "--cases",
        default=str(Path(__file__).with_name("golden_queries.json")),
    )
    parser.add_argument("--resource-name", default="")
    parser.add_argument("--kernel", default="")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--min-pass-rate", type=float, default=1.0)
    parser.add_argument("--min-passed", type=int, default=None)
    parser.add_argument("--summary-file", default="")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cases = json.loads(Path(args.cases).read_text(encoding="utf-8"))
    if args.resource_name:
        for case in cases:
            if isinstance(case, dict):
                case.setdefault("resource_name", args.resource_name)
    if args.kernel:
        for case in cases:
            if isinstance(case, dict):
                case.setdefault("kernel", args.kernel)
    headers = build_headers(args.api_key)
    results = run_cases(cases, base_url=args.base_url, headers=headers, timeout=args.timeout)
    summary = summarize_results(
        results,
        min_pass_rate=args.min_pass_rate,
        min_passed=args.min_passed,
    )

    if args.summary_file:
        Path(args.summary_file).write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    if args.verbose:
        for result in results:
            status = "PASS" if result["passed"] else "FAIL"
            sys.stdout.write(f"[{status}] {result['question']}\n")
            for error in result.get("errors", []):
                sys.stdout.write(f"  - {error}\n")

    output = {
        "success": summary["success"],
        "total": summary["total"],
        "passed": summary["passed"],
        "failed": summary["failed"],
        "pass_rate": summary["pass_rate"],
        "thresholds": summary["thresholds"],
        "failures": summary["failures"],
    }
    sys.stdout.write(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    return 0 if summary["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
