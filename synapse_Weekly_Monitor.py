
import os
import csv
import requests

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from azure.identity import ClientSecretCredential


# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SYNAPSE_ENDPOINT = os.environ["SYNAPSE_ENDPOINT"].rstrip("/")

API_VERSION = "2020-12-01"
SYNAPSE_SCOPE = "https://dev.azuresynapse.net/.default"
LOCAL_TZ = ZoneInfo("Asia/Kolkata")

CSV_FILE = "synapse_pipeline_results.csv"

MONITORED_PIPELINES = [
    "PL_Captura_Snapshots",
    "BronzeToBi_daily_5am",
    "BronzeToBi_daily_530_am",
    "BronzeToBi_daily_530_am_1",
]

PIPELINE_ORDER = {
    name.lower(): index
    for index, name in enumerate(MONITORED_PIPELINES)
}

COLUMNS = [
    "Pipeline name",
    "Run start (IST)",
    "Run end (IST)",
    "Duration",
    "Triggered by",
    "Status",
    "Run ID",
    "Error / Message",
]


# ============================================================
# AZURE AUTHENTICATION
# ============================================================

def get_access_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    return credential.get_token(SYNAPSE_SCOPE).token


# ============================================================
# DATETIME HELPERS
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return None


def to_ist(value):
    dt = parse_datetime(value)

    if dt is None:
        return None

    return dt.astimezone(LOCAL_TZ)


def format_datetime(value):
    dt = to_ist(value)

    if dt is None:
        return "-"

    return dt.strftime("%d/%m/%Y %I:%M:%S %p")


def calculate_duration(run):
    start = parse_datetime(run.get("runStart"))
    end = parse_datetime(run.get("runEnd"))

    if start is None:
        return "-"

    # A running job has no final duration yet.
    if end is None:
        return "In progress"

    seconds = max(
        0, int((end - start).total_seconds())
    )

    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"{hours:02}:{minutes:02}:{seconds:02}"


# ============================================================
# TRIGGER DETAILS
# ============================================================

def get_triggered_by(run):
    invoked_by = run.get("invokedBy") or {}

    if isinstance(invoked_by, dict):
        return (
            invoked_by.get("name")
            or invoked_by.get("pipelineName")
            or invoked_by.get("id")
            or "-"
        )

    return str(invoked_by) if invoked_by else "-"


# ============================================================
# ERROR / MESSAGE DETAILS
# ============================================================

def get_error_message(run):
    message = run.get("message")

    if message:
        return str(message)

    error = run.get("error")

    if isinstance(error, dict):
        return str(
            error.get("message")
            or error.get("code")
            or "-"
        )

    if error:
        return str(error)

    return "-"


# ============================================================
# FETCH ALL PAGES OF SYNAPSE RUNS
# ============================================================

def get_pipeline_runs():
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    url = (
        f"{SYNAPSE_ENDPOINT}/queryPipelineRuns"
        f"?api-version={API_VERSION}"
    )

    payload = {
        "lastUpdatedAfter": cutoff.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "lastUpdatedBefore": now.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "filters": [
            {
                "operand": "PipelineName",
                "operator": "In",
                "values": MONITORED_PIPELINES,
            }
        ],
        "orderBy": [
            {
                "orderBy": "RunStart",
                "order": "DESC",
            }
        ],
    }

    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json",
    }

    all_runs = []
    continuation_token = None

    with requests.Session() as session:
        while True:
            if continuation_token:
                payload["continuationToken"] = continuation_token
            else:
                payload.pop("continuationToken", None)

            response = session.post(
                url,
                headers=headers,
                json=payload,
                timeout=60,
            )
            response.raise_for_status()

            data = response.json()
            page_runs = data.get("value", [])
            all_runs.extend(page_runs)

            print(
                f"Fetched page: {len(page_runs)} runs; "
                f"total retrieved: {len(all_runs)}"
            )

            continuation_token = data.get("continuationToken")

            if not continuation_token:
                break

    return all_runs, cutoff, now


# ============================================================
# FILTER THE FOUR JOBS BY ACTUAL START TIME
# ============================================================

def filter_monitored_runs(all_runs, cutoff, now):
    monitored_names = {
        name.lower() for name in MONITORED_PIPELINES
    }

    filtered = []

    for run in all_runs:
        pipeline_name = run.get("pipelineName", "")

        if pipeline_name.lower() not in monitored_names:
            continue

        start = parse_datetime(run.get("runStart"))

        if start is None:
            continue

        # Include only runs started within the last 24 hours.
        if cutoff <= start <= now:
            filtered.append(run)

    filtered.sort(
        key=lambda run: (
            PIPELINE_ORDER.get(
                run.get("pipelineName", "").lower(), 999
            ),
            run.get("runStart") or "",
        )
    )

    return filtered


# ============================================================
# BUILD REPORT ROWS
# ============================================================

def build_report(runs):
    rows = []

    for run in runs:
        rows.append({
            "Pipeline name": run.get("pipelineName", "-"),
            "Run start (IST)": format_datetime(
                run.get("runStart")
            ),
            "Run end (IST)": format_datetime(
                run.get("runEnd")
            ),
            "Duration": calculate_duration(run),
            "Triggered by": get_triggered_by(run),
            "Status": run.get("status", "Unknown"),
            "Run ID": run.get("runId", "-"),
            "Error / Message": get_error_message(run),
        })

    return rows


# ============================================================
# PRINT RESULTS IN TABLE FORMAT
# ============================================================

def print_table(rows):
    print("\nSYNAPSE JOB MONITORING — LAST 24 HOURS")
    print("=" * 120)

    if not rows:
        print("No matching pipeline runs found in the last 24 hours.")
        return

    widths = {
        column: max(
            len(column),
            max(len(str(row.get(column, ""))) for row in rows),
        )
        for column in COLUMNS
    }

    print(
        " | ".join(
            column.ljust(widths[column])
            for column in COLUMNS
        )
    )

    print(
        "-+-".join(
            "-" * widths[column]
            for column in COLUMNS
        )
    )

    for row in rows:
        print(
            " | ".join(
                str(row.get(column, "")).ljust(widths[column])
                for column in COLUMNS
            )
        )

    print(f"\nTotal matching runs: {len(rows)}")


# ============================================================
# EXPORT CSV
# ============================================================

def create_csv(rows):
    with open(
        CSV_FILE,
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=COLUMNS,
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV generated: {CSV_FILE}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("SYNAPSE UNATTENDED MONITOR")
    print("=" * 80)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    print(f"Current time (IST): {now.astimezone(LOCAL_TZ):%d/%m/%Y %I:%M:%S %p}")
    print(f"Window start (IST): {cutoff.astimezone(LOCAL_TZ):%d/%m/%Y %I:%M:%S %p}")
    print("Monitoring all four configured pipelines.\n")

    all_runs, cutoff, now = get_pipeline_runs()

    print(f"\nTotal API runs retrieved: {len(all_runs)}")

    monitored_runs = filter_monitored_runs(
        all_runs, cutoff, now
    )

    print(f"Matching runs in last 24 hours: {len(monitored_runs)}")

    rows = build_report(monitored_runs)

    print_table(rows)
    create_csv(rows)

    print("\nMonitoring completed successfully.")


if __name__ == "__main__":
    main()
