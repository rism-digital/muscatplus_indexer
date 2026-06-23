import argparse
import logging
import logging.config
import sys
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import orjson
import yaml
from pyreqwest.client import SyncClientBuilder

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EXTERNAL_IDS: dict[str, dict[str, str]] = {
    "viaf": {
        "label": "Virtual Internet Authority File (VIAF)",
        "ident": "https://viaf.org/viaf/{ident}",
    },
    "dnb": {
        "label": "Deutsche Nationalbibliothek (GND)",
        "ident": "https://d-nb.info/gnd/{ident}",
    },
    "wkp": {"label": "Wikidata", "ident": "https://www.wikidata.org/wiki/{ident}"},
    "isil": {
        "label": "International Standard Identifier for Libraries and Related Organizations (ISIL)"
    },
    "bne": {
        "label": "Biblioteca Nacional de España",
        "ident": "https://datos.bne.es/resource/{ident}",
    },
    "bnf": {
        "label": "Bibliothèque Nationale de France",
        "ident": "https://ark.bnf.fr/{ident}",
    },
    "iccu": {
        "label": "Istituto Centrale per il Catalogo Unico",
        "ident": "http://id.sbn.it/bid/{ident}",
    },
    "isni": {
        "label": "International Standard Name Identifier",
        "ident": "https://isni.org/isni/{ident}",
    },
    "lc": {
        "label": "Library of Congress",
        "ident": "http://id.loc.gov/authorities/names/{ident}",
    },
    "nlp": {
        "label": "Biblioteka Narodowa",
        "ident": "https://dbn.bn.org.pl/descriptor-details/{ident}",
    },
    "nkc": {
        "label": "Národní knihovna České republiky",
        "ident": "https://aleph.nkp.cz/F/?func=find-c&local_base=aut&ccl_term=ica={ident}",
    },
    "swnl": {"label": "Schweizerische Nationalbibliothek"},
    "moc": {"label": "MARC Organization Code"},
    "orcid": {
        "label": "Open Researcher and Contributor ID (ORCiD)",
        "ident": "https://orcid.org/{ident}",
    },
    "diamm": {
        "label": "Digital Image Archive of Medieval Music",
        "ident": "https://www.diamm.ac.uk/{ident}",
    },
    "cantus": {
        "label": "Cantus: A Database for Latin Ecclesiastical Chant",
        "ident": "https://cantusdatabase.org/{ident}",
    },
    "cmo": {
        "label": "Corpus Musicae Ottomanicae (CMO)",
        "ident": "https://corpus-musicae-ottomanicae.de/receive/{ident}",
    },
    "tgn": {
        "label": "Getty Thesaurus of Geographic Names",
        "ident": "https://vocab.getty.edu/tgn/{ident}",
    },
    "corago": {
        "label": "Corago: Repertorio e archivio di libretti del melodramma italiano dal 1600 al 1900"
    },
    "oclc": {
        "label": "OCLC Entities",
        "ident": "https://entities.oclc.org/worldcat/entity/{ident}",
    },
}


log = logging.getLogger("authority_link_checker")


DEFAULT_USER_AGENT = (
    "RISM Authority Checker Bot/1.0 "
    "(https://rism.digital, info@rism.digital) "
    "pyreqwest/0.11.8"
)
DEFAULT_ROWS = 500
DEFAULT_TIMEOUT = 20
DEFAULT_WORKERS = 2
DEFAULT_RETRIES = 2
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
UPDATE_RETRY_STATUSES = {429, 503}
SERVICE_TIMEOUTS: dict[str, int] = {"lc": 40}
SERVICE_MIN_INTERVALS: dict[str, float] = {"lc": 1.0, "viaf": 1.0, "iccu": 1.0}
RETRYABLE_EXCEPTION_NAMES = {"ConnectTimeoutError"}
IGNORE_RETRY_AFTER_SERVICES = {"viaf"}
MAX_RETRY_DELAY_SECONDS = 30.0
REQUEST_TIMESTAMPS: dict[str, float] = {}
REQUEST_TIMESTAMPS_LOCK = threading.Lock()
HEARTBEAT_SECONDS = 10


def configure_logging(debug: bool) -> None:
    config_path = Path(__file__).resolve().parents[1] / "logging.yml"
    log_config: dict[str, Any] = yaml.full_load(config_path.read_text())
    logging.config.dictConfig(log_config)
    if debug:
        log.setLevel(logging.DEBUG)


def print_debug(message: str) -> None:
    log.debug(message)


def print_progress(message: str, level: int = logging.INFO) -> None:
    log.log(level, message)


@dataclass(frozen=True)
class RecordReference:
    rism_id: str
    full_rism_id: str
    record_type: str


@dataclass(frozen=True)
class ValidationResult:
    service: str
    identifier: str
    external_id: str
    url: str | None
    ok: bool
    skipped: bool
    failure_type: str | None
    reason: str | None
    http_status: int | None


def parse_external_id(external_id: str) -> tuple[str, str]:
    if ":" not in external_id:
        raise ValueError(f"Malformed external id: {external_id}")

    service, identifier = external_id.split(":", 1)
    service = service.strip().lower()
    identifier = identifier.strip()
    if not service or not identifier:
        raise ValueError(f"Malformed external id: {external_id}")

    return service, identifier


def build_authority_url(service: str, identifier: str) -> str | None:
    service_config = EXTERNAL_IDS.get(service)
    if not service_config:
        return None

    template = service_config.get("ident")
    if not template:
        return None

    return template.format(ident=identifier)


def build_request_url(service: str, identifier: str) -> str | None:
    authority_url = build_authority_url(service, identifier)
    if not authority_url:
        return None

    if service == "isni":
        query = urlencode(
            {
                "query": f'pica.isn = "{identifier}"',
                "operation": "searchRetrieve",
                "recordSchema": "isni-b",
            }
        )
        return f"http://isni.oclc.org/sru/DB=1.2/?{query}"
    if service == "lc":
        return f"{authority_url}.skos.json"
    if service == "bne":
        return f"{authority_url}.jsonld"

    return authority_url


def get_retry_delay(response: Any, attempt: int, service: str) -> float:
    retry_after = None
    if service not in IGNORE_RETRY_AFTER_SERVICES:
        retry_after = response.get_header("retry-after")
    if retry_after:
        try:
            return min(MAX_RETRY_DELAY_SECONDS, max(0.0, float(retry_after)))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                now = datetime.now(retry_at.tzinfo or UTC)
                return min(
                    MAX_RETRY_DELAY_SECONDS,
                    max(0.0, (retry_at - now).total_seconds()),
                )
            except (TypeError, ValueError, IndexError, OverflowError):
                pass

    return min(MAX_RETRY_DELAY_SECONDS, 0.5 * attempt)


def get_exception_retry_delay(attempt: int) -> float:
    return min(MAX_RETRY_DELAY_SECONDS, 0.5 * attempt)


def wait_for_service_rate_limit(service: str) -> None:
    minimum_interval = SERVICE_MIN_INTERVALS.get(service)
    if not minimum_interval:
        return

    with REQUEST_TIMESTAMPS_LOCK:
        now = time.monotonic()
        last_request_time = REQUEST_TIMESTAMPS.get(service)
        if last_request_time is not None:
            elapsed = now - last_request_time
            if elapsed < minimum_interval:
                wait_time = minimum_interval - elapsed
                print_debug(
                    f"rate limiting {service} for {wait_time:.2f}s before next request"
                )
                time.sleep(wait_time)
                now = time.monotonic()

        REQUEST_TIMESTAMPS[service] = now


def default_validator(
    response: Any, identifier: str, url: str, external_id: str
) -> ValidationResult:
    text = response.text()
    lowered_text = text.lower()
    if (
        response.status == 403
        and "cloudflare" in lowered_text
        and "you have been blocked" in lowered_text
    ):
        return ValidationResult(
            service=external_id.split(":", 1)[0],
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=False,
            skipped=False,
            failure_type="service_blocked",
            reason="Remote service blocked automated access.",
            http_status=response.status,
        )

    if 200 <= response.status < 400:
        return ValidationResult(
            service=external_id.split(":", 1)[0],
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=True,
            skipped=False,
            failure_type=None,
            reason=None,
            http_status=response.status,
        )

    return ValidationResult(
        service=external_id.split(":", 1)[0],
        identifier=identifier,
        external_id=external_id,
        url=url,
        ok=False,
        skipped=False,
        failure_type="http_error",
        reason=f"Unexpected HTTP status {response.status}.",
        http_status=response.status,
    )


def wikidata_validator(
    response: Any, identifier: str, url: str, external_id: str
) -> ValidationResult:
    default_result = default_validator(response, identifier, url, external_id)
    if not default_result.ok:
        return default_result

    text = response.text().lower()
    if "this entity does not exist" in text:
        return ValidationResult(
            service="wkp",
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=False,
            skipped=False,
            failure_type="soft_404",
            reason="Wikidata returned a page stating that the entity does not exist.",
            http_status=response.status,
        )

    return default_result


def orcid_validator(
    response: Any, identifier: str, url: str, external_id: str
) -> ValidationResult:
    if 200 <= response.status < 400:
        try:
            body = response.json()
        except Exception as exc:
            return ValidationResult(
                service="orcid",
                identifier=identifier,
                external_id=external_id,
                url=url,
                ok=False,
                skipped=False,
                failure_type="request_error",
                reason=f"Could not parse ORCID response JSON: {exc}",
                http_status=response.status,
            )

        path = (
            body.get("orcid-identifier", {}).get("path")
            if isinstance(body, dict)
            else None
        )
        if path == identifier:
            return ValidationResult(
                service="orcid",
                identifier=identifier,
                external_id=external_id,
                url=url,
                ok=True,
                skipped=False,
                failure_type=None,
                reason=None,
                http_status=response.status,
            )

        return ValidationResult(
            service="orcid",
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=False,
            skipped=False,
            failure_type="soft_404",
            reason="ORCID response did not resolve to the requested identifier.",
            http_status=response.status,
        )

    return ValidationResult(
        service="orcid",
        identifier=identifier,
        external_id=external_id,
        url=url,
        ok=False,
        skipped=False,
        failure_type="http_error",
        reason=f"Unexpected HTTP status {response.status}.",
        http_status=response.status,
    )


def isni_validator(
    response: Any, identifier: str, url: str, external_id: str
) -> ValidationResult:
    if not (200 <= response.status < 400):
        return default_validator(response, identifier, url, external_id)

    try:
        root = ET.fromstring(response.text())
    except ET.ParseError as exc:
        return ValidationResult(
            service="isni",
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=False,
            skipped=False,
            failure_type="request_error",
            reason=f"Could not parse ISNI SRU XML: {exc}",
            http_status=response.status,
        )

    number_of_records = root.findtext(".//{http://www.loc.gov/zing/srw/}numberOfRecords")
    if number_of_records == "0":
        return ValidationResult(
            service="isni",
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=False,
            skipped=False,
            failure_type="soft_404",
            reason="ISNI SRU returned no matching records.",
            http_status=response.status,
        )

    returned_isnis = {
        element.text.strip()
        for element in root.findall(".//isniUnformatted")
        if element.text and element.text.strip()
    }
    merged_isnis = {
        element.text.strip()
        for element in root.findall(".//mergedISNI")
        if element.text and element.text.strip()
    }
    if identifier in returned_isnis or identifier in merged_isnis:
        return ValidationResult(
            service="isni",
            identifier=identifier,
            external_id=external_id,
            url=url,
            ok=True,
            skipped=False,
            failure_type=None,
            reason=None,
            http_status=response.status,
        )

    return ValidationResult(
        service="isni",
        identifier=identifier,
        external_id=external_id,
        url=url,
        ok=False,
        skipped=False,
        failure_type="soft_404",
        reason="ISNI SRU response did not contain the requested identifier.",
        http_status=response.status,
    )


VALIDATORS: dict[str, Any] = {
    "isni": isni_validator,
    "wkp": wikidata_validator,
    "orcid": orcid_validator,
}

REQUEST_HEADERS: dict[str, dict[str, str]] = {
    "dnb": {"Accept": "application/ld+json"},
    "isni": {"Accept": "application/xml"},
    "viaf": {"Accept": "application/json"},
    "orcid": {"Accept": "application/vnd.orcid+json"},
}


def validate_external_id(
    external_id: str, timeout: int = DEFAULT_TIMEOUT, retries: int = DEFAULT_RETRIES
) -> ValidationResult:
    try:
        service, identifier = parse_external_id(external_id)
    except ValueError as exc:
        return ValidationResult(
            service="",
            identifier="",
            external_id=external_id,
            url=None,
            ok=False,
            skipped=False,
            failure_type="invalid_identifier",
            reason=str(exc),
            http_status=None,
        )

    service_config = EXTERNAL_IDS.get(service)
    if service_config is None:
        return ValidationResult(
            service=service,
            identifier=identifier,
            external_id=external_id,
            url=None,
            ok=False,
            skipped=False,
            failure_type="missing_mapping",
            reason=f"No resolver mapping is configured for service '{service}'.",
            http_status=None,
        )

    authority_url = build_authority_url(service, identifier)
    request_url = build_request_url(service, identifier)
    if not authority_url or not request_url:
        return ValidationResult(
            service=service,
            identifier=identifier,
            external_id=external_id,
            url=None,
            ok=True,
            skipped=True,
            failure_type="missing_url_template",
            reason=f"Service '{service}' has no URL template configured.",
            http_status=None,
        )

    validator = VALIDATORS.get(service, default_validator)
    headers = REQUEST_HEADERS.get(service, {})

    request_timeout = SERVICE_TIMEOUTS.get(service, timeout)
    print_debug(
        f"prepared {external_id} service={service} timeout={request_timeout}s "
        f"request_url={request_url}"
    )

    attempt = 0
    while True:
        try:
            wait_for_service_rate_limit(service)
            started_at = time.perf_counter()
            print_debug(
                f"request start {external_id} attempt={attempt + 1} "
                f"timeout={request_timeout}s"
            )
            with (
                SyncClientBuilder()
                .gzip(True)
                .deflate(True)
                .follow_redirects(True)
                .timeout(timedelta(seconds=request_timeout))
                .user_agent(DEFAULT_USER_AGENT)
                .build() as client
            ):
                response = client.get(request_url).headers(headers).build().send()
            elapsed = time.perf_counter() - started_at
            print_debug(
                f"request end {external_id} attempt={attempt + 1} "
                f"status={response.status} elapsed={elapsed:.2f}s"
            )
        except Exception as exc:
            if (
                exc.__class__.__name__ in RETRYABLE_EXCEPTION_NAMES
                and attempt < retries
            ):
                attempt += 1
                retry_delay = get_exception_retry_delay(attempt)
                print_debug(
                    f"request exception retry {external_id} attempt={attempt} "
                    f"exception={exc.__class__.__name__} sleeping={retry_delay:.2f}s"
                )
                time.sleep(retry_delay)
                continue

            print_debug(
                f"request exception final {external_id} "
                f"exception={exc.__class__.__name__} detail={exc}"
            )
            return ValidationResult(
                service=service,
                identifier=identifier,
                external_id=external_id,
                url=authority_url,
                ok=False,
                skipped=False,
                failure_type="request_error",
                reason=str(exc),
                http_status=None,
            )

        if response.status not in RETRYABLE_STATUSES or attempt >= retries:
            break

        attempt += 1
        retry_delay = get_retry_delay(response, attempt, service)
        print_debug(
            f"http retry {external_id} attempt={attempt} status={response.status} "
            f"sleeping={retry_delay:.2f}s"
        )
        time.sleep(retry_delay)

    return validator(response, identifier, authority_url, external_id)


def fetch_solr_documents(
    solr_url: str,
    rows: int,
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> tuple[dict[str, list[RecordReference]], int]:
    from pyreqwest.client import SyncClientBuilder

    grouped_ids: dict[str, list[RecordReference]] = defaultdict(list)
    documents_scanned = 0
    cursor = "*"
    page_number = 0

    with (
        SyncClientBuilder()
        .gzip(True)
        .deflate(True)
        .follow_redirects(True)
        .build() as client
    ):
        while True:
            page_number += 1
            url = (
                f"{solr_url}/select?q=external_ids:*&rows={rows}"
                "&fl=rism_id,full_rism_id,type,external_ids"
                f"&sort=id asc&cursorMark={cursor}&wt=json"
            )
            response = client.get(url).build().send()
            if not (200 <= response.status < 400):
                raise RuntimeError(
                    f"Could not query Solr. Status {response.status}: {response.text()}"
                )

            body = response.json()
            docs = body.get("response", {}).get("docs", [])
            documents_scanned += len(docs)
            print_progress(
                f"[solr] page {page_number}: fetched {len(docs)} docs "
                f"({documents_scanned} total)"
            )
            for doc in docs:
                rism_id = str(doc.get("rism_id", ""))
                full_rism_id = str(doc.get("full_rism_id", ""))
                record_type = str(doc.get("type", ""))
                record_reference = RecordReference(
                    rism_id=rism_id,
                    full_rism_id=full_rism_id,
                    record_type=record_type,
                )
                for external_id in doc.get("external_ids", []):
                    try:
                        service, _ = parse_external_id(external_id)
                    except ValueError:
                        grouped_ids[external_id].append(record_reference)
                        continue

                    if skipped_services and service in skipped_services:
                        continue

                    if selected_services and service not in selected_services:
                        continue

                    grouped_ids[external_id].append(record_reference)

            next_cursor = body.get("nextCursorMark")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

    return grouped_ids, documents_scanned


def fetch_solr_records_by_full_rism_id(
    solr_url: str, full_rism_ids: set[str], batch_size: int = 200
) -> dict[str, dict[str, Any]]:
    records_by_full_rism_id: dict[str, dict[str, Any]] = {}
    if not full_rism_ids:
        return records_by_full_rism_id

    identifiers = sorted(full_rism_ids)
    with (
        SyncClientBuilder()
        .gzip(True)
        .deflate(True)
        .follow_redirects(True)
        .build() as client
    ):
        for start in range(0, len(identifiers), batch_size):
            chunk = identifiers[start : start + batch_size]
            query = " OR ".join(f'full_rism_id:"{item}"' for item in chunk)
            body = urlencode(
                {
                    "q": query,
                    "fl": "full_rism_id,type,external_ids",
                    "rows": str(len(chunk)),
                    "wt": "json",
                }
            )
            response = (
                client.post(f"{solr_url}/select")
                .headers({"Content-Type": "application/x-www-form-urlencoded"})
                .body_text(body)
                .build()
                .send()
            )
            if not (200 <= response.status < 400):
                raise RuntimeError(
                    f"Could not query Solr. Status {response.status}: {response.text()}"
                )

            body = response.json()
            docs = body.get("response", {}).get("docs", [])
            for doc in docs:
                full_rism_id = str(doc.get("full_rism_id", ""))
                if full_rism_id:
                    records_by_full_rism_id[full_rism_id] = doc

    return records_by_full_rism_id


def is_update_candidate(
    failure: dict[str, Any],
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> bool:
    if not service_matches_filters(failure, selected_services, skipped_services):
        return False
    if failure.get("http_status") in UPDATE_RETRY_STATUSES:
        return True
    return (
        failure.get("failure_type") == "request_error"
        and failure.get("http_status") is None
    )


def service_matches_filters(
    failure: dict[str, Any],
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> bool:
    service = str(failure.get("service", "")).strip().lower()
    if skipped_services and service in skipped_services:
        return False
    if selected_services and service not in selected_services:
        return False
    return True


def build_grouped_ids_from_failures(
    failures: list[dict[str, Any]],
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> dict[str, list[RecordReference]]:
    grouped_ids: dict[str, list[RecordReference]] = defaultdict(list)
    for failure in failures:
        if not service_matches_filters(failure, selected_services, skipped_services):
            continue

        external_id = str(failure.get("external_id", "")).strip()
        if not external_id:
            continue

        grouped_ids[external_id].append(
            RecordReference(
                rism_id=str(failure.get("rism_id", "")),
                full_rism_id=str(failure.get("full_rism_id", "")),
                record_type=str(failure.get("record_type", "")),
            )
        )

    return grouped_ids


def build_grouped_ids_from_refreshed_failures(
    failures: list[dict[str, Any]],
    records_by_full_rism_id: dict[str, dict[str, Any]],
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> tuple[dict[str, list[RecordReference]], set[tuple[str, str]]]:
    grouped_ids: dict[str, list[RecordReference]] = defaultdict(list)
    removed_keys: set[tuple[str, str]] = set()

    for failure in failures:
        if not service_matches_filters(failure, selected_services, skipped_services):
            continue

        full_rism_id = str(failure.get("full_rism_id", ""))
        external_id = str(failure.get("external_id", "")).strip()
        if not full_rism_id or not external_id:
            continue

        record = records_by_full_rism_id.get(full_rism_id)
        if record is None:
            continue

        current_external_ids = record.get("external_ids") or []
        if external_id not in current_external_ids:
            removed_keys.add((full_rism_id, external_id))
            continue

        grouped_ids[external_id].append(
            RecordReference(
                rism_id=str(failure.get("rism_id", "")),
                full_rism_id=full_rism_id,
                record_type=str(record.get("type") or failure.get("record_type", "")),
            )
        )

    return grouped_ids, removed_keys


def build_failures(
    grouped_ids: dict[str, list[RecordReference]],
    timeout: int,
    workers: int,
    retries: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    service_failures: Counter[str] = Counter()
    service_skipped: Counter[str] = Counter()
    service_checked: Counter[str] = Counter()
    checked_links = 0
    skipped_links = 0
    completed = 0
    total_unique = len(grouped_ids)

    print_progress(
        f"[check] validating {total_unique} unique external ids with {workers} workers"
    )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                validate_external_id, external_id, timeout, retries
            ): external_id
            for external_id in grouped_ids
        }
        pending = set(future_map)
        while pending:
            done, pending = wait(
                pending, timeout=HEARTBEAT_SECONDS, return_when=FIRST_COMPLETED
            )
            if not done:
                sample = ", ".join(future_map[future] for future in list(pending)[:3])
                print_progress(
                    f"[check] heartbeat: {completed}/{total_unique} complete, "
                    f"{len(pending)} still running"
                    + (f" (sample: {sample})" if sample else "")
                )
                continue

            for future in done:
                external_id = future_map[future]
                references = grouped_ids[external_id]
                result = future.result()
                multiplicity = len(references)
                record_paths = ", ".join(
                    reference.full_rism_id for reference in references
                )
                completed += 1

                if result.skipped:
                    service_skipped[result.service] += multiplicity
                    skipped_links += multiplicity
                    print_progress(
                        f"[check] {completed}/{total_unique} skipped {external_id}: "
                        f"{result.reason}"
                    )
                    continue

                checked_links += multiplicity
                service_checked[result.service] += multiplicity
                if result.ok:
                    if completed == total_unique or completed % 25 == 0:
                        print_progress(
                            f"[check] {completed}/{total_unique} complete; "
                            f"{len(failures)} failures so far"
                        )
                    continue

                service_failures[result.service] += multiplicity
                for reference in references:
                    failures.append(
                        {
                            "rism_id": reference.rism_id,
                            "full_rism_id": reference.full_rism_id,
                            "record_type": reference.record_type,
                            "external_id": result.external_id,
                            "service": result.service,
                            "identifier": result.identifier,
                            "url": result.url,
                            "failure_type": result.failure_type,
                            "http_status": result.http_status,
                            "reason": result.reason,
                        }
                    )
                print_progress(
                    f"[check] {completed}/{total_unique} failed {external_id} "
                    f"(Records: {record_paths}): {result.reason}"
                )

    summary = {
        "links_checked": checked_links,
        "links_failed": len(failures),
        "links_skipped": skipped_links,
        "services_checked": dict(service_checked),
        "service_failures": dict(service_failures),
        "service_skipped": dict(service_skipped),
    }
    return failures, summary


def choose_solr_core(config: dict, requested_core: str | None) -> str:
    if requested_core:
        return requested_core
    return config["solr"]["live_core"]


def parse_services(raw_services: str | None) -> set[str] | None:
    if not raw_services:
        return None
    return {
        service.strip().lower()
        for service in raw_services.split(",")
        if service.strip()
    }


def parse_skipped_services(raw_services: str | None) -> set[str] | None:
    return parse_services(raw_services)


def build_report(
    solr_core: str,
    documents_scanned: int,
    failures: list[dict[str, Any]],
    summary: dict[str, Any],
    started_at: float,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "duration_seconds": round(time.perf_counter() - started_at, 3),
        "solr_core": solr_core,
        "documents_scanned": documents_scanned,
        "links_checked": summary["links_checked"],
        "links_failed": summary["links_failed"],
        "links_skipped": summary["links_skipped"],
        "services_checked": summary["services_checked"],
        "service_failures": summary["service_failures"],
        "service_skipped": summary["service_skipped"],
        "failures": failures,
    }


def load_report(path: Path) -> dict[str, Any]:
    report = orjson.loads(path.read_bytes())
    if not isinstance(report, dict):
        raise ValueError("Report JSON must be an object.")
    failures = report.get("failures")
    if not isinstance(failures, list):
        raise ValueError("Report JSON must contain a failures list.")
    return report


def summarize_failures(failures: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for failure in failures:
        service = str(failure.get("service", "")).strip().lower()
        if service:
            counts[service] += 1
    return dict(counts)


def merge_updated_failures(
    existing_failures: list[dict[str, Any]],
    updated_failures: list[dict[str, Any]],
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> list[dict[str, Any]]:
    updated_by_key = {
        (failure.get("full_rism_id"), failure.get("external_id")): failure
        for failure in updated_failures
    }
    merged_failures: list[dict[str, Any]] = []

    for failure in existing_failures:
        if not service_matches_filters(failure, selected_services, skipped_services):
            merged_failures.append(failure)
            continue

        key = (failure.get("full_rism_id"), failure.get("external_id"))
        if key in updated_by_key:
            merged_failures.append(updated_by_key.pop(key))

    merged_failures.extend(updated_by_key.values())
    return merged_failures


def merge_refreshed_failures(
    existing_failures: list[dict[str, Any]],
    refreshed_failures: list[dict[str, Any]],
    removed_keys: set[tuple[str, str]],
    selected_services: set[str] | None,
    skipped_services: set[str] | None,
) -> list[dict[str, Any]]:
    refreshed_by_key = {
        (failure.get("full_rism_id"), failure.get("external_id")): failure
        for failure in refreshed_failures
    }
    merged_failures: list[dict[str, Any]] = []

    for failure in existing_failures:
        if not service_matches_filters(failure, selected_services, skipped_services):
            merged_failures.append(failure)
            continue

        key = (failure.get("full_rism_id"), failure.get("external_id"))
        if key in removed_keys:
            continue
        if key in refreshed_by_key:
            merged_failures.append(refreshed_by_key.pop(key))
            continue

    merged_failures.extend(refreshed_by_key.values())
    return merged_failures


def build_updated_report(
    report: dict[str, Any],
    updated_failures: list[dict[str, Any]],
    started_at: float,
) -> dict[str, Any]:
    updated_report = dict(report)
    updated_report["generated_at"] = datetime.now(UTC).isoformat()
    updated_report["duration_seconds"] = round(time.perf_counter() - started_at, 3)
    updated_report["failures"] = updated_failures
    updated_report["links_failed"] = len(updated_failures)
    updated_report["service_failures"] = summarize_failures(updated_failures)
    return updated_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check external authority links indexed in Solr and emit a JSON report."
    )
    parser.add_argument(
        "--config",
        default="index_config.yml",
        help="Path to the indexer configuration file.",
    )
    parser.add_argument(
        "--core",
        help="Override the Solr core to query. Defaults to the live core in the config.",
    )
    parser.add_argument(
        "--output",
        default="authority_link_failures.json",
        help="Path to the JSON report to write.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help="Number of Solr rows to fetch per cursor page.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="HTTP timeout per authority check, in seconds.",
    )
    parser.add_argument(
        "--max-links",
        type=int,
        help="Stop after validating this many unique external ids.",
    )
    parser.add_argument(
        "--services",
        help="Comma-separated list of service prefixes to check.",
    )
    parser.add_argument(
        "--skip",
        help="Comma-separated list of service prefixes to skip.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of worker threads to use when checking unique external ids.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Number of retries for retryable HTTP statuses such as 429.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print detailed request, retry, and heartbeat debugging information.",
    )
    parser.add_argument(
        "--update",
        help="Update an existing JSON report in place by retrying matching failures from the report.",
    )
    parser.add_argument(
        "--refresh",
        help="Refresh an existing JSON report in place by re-fetching failed records from Solr before retrying current identifiers.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(args.debug)
    selected_services = parse_services(args.services)
    skipped_services = parse_skipped_services(args.skip)
    started_at = time.perf_counter()

    if args.update:
        update_path = Path(args.update)
        report = load_report(update_path)
        grouped_ids = build_grouped_ids_from_failures(
            report.get("failures", []),
            selected_services=selected_services,
            skipped_services=skipped_services,
        )
        print_progress(
            f"[update] found {len(grouped_ids)} external ids to retry in {update_path}"
        )
        if args.max_links is not None:
            grouped_ids = dict(list(grouped_ids.items())[: args.max_links])
            print_progress(
                f"[update] limiting retry pass to first {len(grouped_ids)} unique external ids"
            )
        if not grouped_ids:
            print_progress(
                "[update] no matching failures found; report left unchanged"
            )
            return 0

        retry_failures, _ = build_failures(
            grouped_ids=grouped_ids,
            timeout=args.timeout,
            workers=args.workers,
            retries=args.retries,
        )
        updated_failures = merge_updated_failures(
            existing_failures=report.get("failures", []),
            updated_failures=retry_failures,
            selected_services=selected_services,
            skipped_services=skipped_services,
        )
        updated_report = build_updated_report(
            report=report,
            updated_failures=updated_failures,
            started_at=started_at,
        )
        update_path.write_bytes(
            orjson.dumps(updated_report, option=orjson.OPT_INDENT_2)
        )
        print_progress(
            f"[done] updated {update_path} with {updated_report['links_failed']} remaining failures"
        )
        log.info(
            "Updated %s. %s failures remain.",
            update_path,
            updated_report["links_failed"],
        )
        return 0

    if args.refresh:
        refresh_path = Path(args.refresh)
        report = load_report(refresh_path)
        config = yaml.full_load(Path(args.config).read_text())
        solr_core = choose_solr_core(config, args.core)
        solr_url = f"{config['solr']['server']}/{solr_core}"
        full_rism_ids = {
            str(failure.get("full_rism_id", "")).strip()
            for failure in report.get("failures", [])
            if service_matches_filters(
                failure,
                selected_services,
                skipped_services,
            )
            and str(failure.get("full_rism_id", "")).strip()
        }
        print_progress(
            f"[refresh] fetching {len(full_rism_ids)} Solr records from {solr_core} for {refresh_path}"
        )
        records_by_full_rism_id = fetch_solr_records_by_full_rism_id(
            solr_url=solr_url,
            full_rism_ids=full_rism_ids,
        )
        grouped_ids, removed_keys = build_grouped_ids_from_refreshed_failures(
            report.get("failures", []),
            records_by_full_rism_id=records_by_full_rism_id,
            selected_services=selected_services,
            skipped_services=skipped_services,
        )
        print_progress(
            f"[refresh] found {len(grouped_ids)} external ids to retry and {len(removed_keys)} stale failures to remove"
        )
        if args.max_links is not None:
            grouped_ids = dict(list(grouped_ids.items())[: args.max_links])
            print_progress(
                f"[refresh] limiting retry pass to first {len(grouped_ids)} unique external ids"
            )
        if not grouped_ids and not removed_keys:
            print_progress(
                "[refresh] no matching failures found after Solr refresh; report left unchanged"
            )
            return 0

        refreshed_failures: list[dict[str, Any]] = []
        if grouped_ids:
            refreshed_failures, _ = build_failures(
                grouped_ids=grouped_ids,
                timeout=args.timeout,
                workers=args.workers,
                retries=args.retries,
            )

        merged_failures = merge_refreshed_failures(
            existing_failures=report.get("failures", []),
            refreshed_failures=refreshed_failures,
            removed_keys=removed_keys,
            selected_services=selected_services,
            skipped_services=skipped_services,
        )
        refreshed_report = build_updated_report(
            report=report,
            updated_failures=merged_failures,
            started_at=started_at,
        )
        refresh_path.write_bytes(
            orjson.dumps(refreshed_report, option=orjson.OPT_INDENT_2)
        )
        print_progress(
            f"[done] refreshed {refresh_path} with {refreshed_report['links_failed']} remaining failures"
        )
        log.info(
            "Refreshed %s. %s failures remain.",
            refresh_path,
            refreshed_report["links_failed"],
        )
        return 0

    config = yaml.full_load(Path(args.config).read_text())
    solr_core = choose_solr_core(config, args.core)
    solr_url = f"{config['solr']['server']}/{solr_core}"

    grouped_ids, documents_scanned = fetch_solr_documents(
        solr_url=solr_url,
        rows=args.rows,
        selected_services=selected_services,
        skipped_services=skipped_services,
    )
    print_progress(
        f"[solr] collected {len(grouped_ids)} unique external ids from "
        f"{documents_scanned} documents"
    )
    if args.max_links is not None:
        limited_ids = dict(list(grouped_ids.items())[: args.max_links])
        grouped_ids = limited_ids
        print_progress(
            f"[check] limiting run to first {len(grouped_ids)} unique external ids"
        )

    failures, summary = build_failures(
        grouped_ids=grouped_ids,
        timeout=args.timeout,
        workers=args.workers,
        retries=args.retries,
    )
    report = build_report(
        solr_core=solr_core,
        documents_scanned=documents_scanned,
        failures=failures,
        summary=summary,
        started_at=started_at,
    )
    Path(args.output).write_bytes(orjson.dumps(report, option=orjson.OPT_INDENT_2))
    print_progress(
        f"[done] wrote {args.output} with {report['links_failed']} failures, "
        f"{report['links_checked']} checked, {report['links_skipped']} skipped"
    )
    log.info(
        "Checked %s links across %s documents. %s failures, %s skipped.",
        report["links_checked"],
        documents_scanned,
        report["links_failed"],
        report["links_skipped"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
