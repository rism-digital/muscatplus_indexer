import logging

import pymarc

from indexer.helpers.marc import create_marc
from indexer.helpers.utilities import (
    convert_work_catalogue_status,
    get_creator_name,
    get_people_names,
    get_related_people,
    to_solr_single,
)

log = logging.getLogger("muscat_indexer")


def get_bibliographic_reference_titles(
    references: list[dict] | None,
) -> list[str] | None:
    if not references:
        return None

    ret: list = []
    for r in references:
        ret.append(format_reference(r))

    return ret


def get_bibliographic_references_json(
    record: pymarc.Record,
    field: str,
    references: list[dict] | None,
    control_subf: str = "0",
) -> list[dict] | None:
    if not references:
        log.debug("No bibliographic references, bailing.")
        return None

    if field not in record:
        log.debug("Field %s is not in the record, bailing.", field)
        return None

    refs: dict[str, dict] = {}
    for ref in references:
        rid = str(ref["id"])
        refs[rid] = ref

    outp: list = []
    fields: list[pymarc.Field] = record.get_fields(field)
    for ff in fields:
        if not ff.subfields:
            log.warning("Empty field %s. Skipping: %s", field, record["001"].value())
            continue

        fid: str | None = ff.get(control_subf)
        if not fid:
            log.warning(
                f"No field {control_subf} for entry in record %s. Skipping: %s",
                record["001"].value(),
                str(ff),
            )
            continue

        if fid not in refs:
            log.warning(
                "The publication ID %s was not available in the list of references for %s. Skipping it.",
                str(fid),
                record["001"].value(),
            )
            continue

        ref = refs[fid]

        publication_id: str = f"publication_{fid}"
        lit = {
            "id": publication_id,
            "formatted": format_reference(ref),
            "work_catalogue_status": (
                convert_work_catalogue_status(t)
                if (t := ref.get("work_catalogue_status"))
                else None
            ),
            "short_name": ref.get("short_name"),
            "title": ref.get("title"),
        }
        if p := ff.get("n"):
            lit["pages"] = p

        if b := ff.get("b"):
            lit["info"] = b

        outp.append({k: v for k, v in lit.items() if v})

    log.debug("Success for field %s, record %s", field, record["001"].value())
    return outp


def reference_author(marc_ref: pymarc.Record) -> str:
    author: str | None = get_creator_name(marc_ref, suppress_dates=True)

    additional_authors_struct: list[dict] = (
        get_related_people(marc_ref, record_id="", record_type="", fields=("700",))
        or []
    )
    filt_add_auth = [
        f for f in additional_authors_struct if f.get("relationship") == "aut"
    ]
    filt_edt = [f for f in additional_authors_struct if f.get("relationship") == "edt"]
    additional_authors: list = get_people_names(filt_add_auth, suppress_dates=True)
    editors: list = get_people_names(filt_edt, suppress_dates=True)
    rel_corp: str | None = to_solr_single(marc_ref, "710", "a")

    if author and additional_authors:
        return f"{author}; {'; '.join(additional_authors)}."
    elif author:
        return f"{author}."
    elif not author and filt_edt:
        # Maybe we have an editor?
        return f"{'; '.join(editors)} ({'ed.' if len(editors) == 1 else 'eds.'})."
    elif not author and rel_corp:
        # Maybe a corporate author?
        return f"{rel_corp}."
    else:
        return "[No author]."


def reference_title(marc_ref: pymarc.Record) -> str:
    reftype = to_solr_single(marc_ref, "240", "h")
    title = to_solr_single(marc_ref, "240", "a")
    if reftype and reftype == "Article/chapter":
        return f"{title}{'' if title.endswith('.') else '.'}" if title else ""
    else:
        return f"<i>{title}{'' if title.endswith('.') else '.'}</i>" if title else ""


def reference_date(marc_ref: pymarc.Record) -> str:
    dt = to_solr_single(marc_ref, "260", "c")

    return f"{dt}{'' if dt.endswith('.') else '.'}" if dt else ""


def reference_place_publisher(marc_ref: pymarc.Record) -> str:
    place = to_solr_single(marc_ref, "260", "a")
    publ = to_solr_single(marc_ref, "260", "b")

    if place and publ:
        return f"{place}: {publ}."
    elif place:
        return f"{place}{'' if place.endswith('.') else '.'}"
    elif publ:
        return f"{publ}{'' if publ.endswith('.') else '.'}"
    return ""


def reference_part_of(marc_ref: pymarc.Record) -> str:
    reftype = to_solr_single(marc_ref, "240", "h")

    series = to_solr_single(marc_ref, "760", "t")
    if reftype and reftype == "Article/chapter":
        f_series = f"<i>{series}</i>"
    else:
        f_series = f"{series}"
    vol_year_page = to_solr_single(marc_ref, "760", "g")

    if series and vol_year_page:
        return f"{f_series}, {vol_year_page}."
    elif series:
        return f"{f_series}."
    elif vol_year_page:
        return f"{vol_year_page}."
    return ""


def reference_shorttitle(marc_ref: pymarc.Record) -> str:
    st = to_solr_single(marc_ref, "210", "a")
    return f"[{st}]" if st else ""


def reference_external_resource(marc_ref: pymarc.Record) -> str:
    ex = to_solr_single(marc_ref, "856", "u")
    return f'<a href="{ex}">{ex}</a>' if ex else ""


def format_reference(ref: dict) -> str:
    res: list = []
    marc_ref = create_marc(ref["marc_source"])

    # short_title: str = reference_shorttitle(marc_ref)
    author: str = reference_author(marc_ref)
    title: str = reference_title(marc_ref)
    date: str = reference_date(marc_ref)
    partof: str = reference_part_of(marc_ref)
    place_pub: str = reference_place_publisher(marc_ref)
    external_resource: str = reference_external_resource(marc_ref)

    # res.append(short_title)
    res.append(author)
    res.append(date)
    res.append(title)
    res.append(partof)
    res.append(place_pub)
    res.append(external_resource)

    out = [r for r in res if r]
    formatted = f"{' '.join(out)}"

    log.debug("record id: %s, formatted: %s", marc_ref["001"].value(), formatted)

    return f"<span>{formatted}{'' if formatted.endswith('.') else '.'}</span>"
