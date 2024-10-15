import logging
from typing import Any, Generator

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool
from diamm_indexer.records.source import create_source_index_documents
from indexer.helpers.solr import record_indexer, submit_to_solr
from indexer.helpers.utilities import parallelise, update_rism_document

log = logging.getLogger("muscat_indexer")


def _get_sources(cfg: dict) -> Generator[list[dict[str, Any]], None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT dds.id AS id, dds.name AS name, dds.shelfmark AS shelfmark, dds.start_date AS start_date,
                dds.end_date AS end_date, dds.date_statement AS date_statement, dds.measurements AS measurements,
                dds.format AS book_format,
                dds.created AS created, dds.updated AS updated, dda.id AS archive_id, dda.name AS archive_name, dda.siglum AS siglum,
                ddg.name AS city_name, ddsa.identifier AS rism_id, ddai.identifier AS archive_rism_identifier,
                (SELECT string_agg(DISTINCT CONCAT(ddoo.name, '||', ddoo.id), '\n') AS organizations
                    FROM diamm_data_sourceprovenance ddop
                    LEFT JOIN diamm_data_organization AS ddoo ON ddop.object_id = ddoo.id
                    WHERE ddop.content_type_id = 52 AND ddop.source_id = dds.id) AS related_organizations,
                (EXISTS(
                    SELECT ddi2.id FROM diamm_data_page ddp2
                    LEFT JOIN public.diamm_data_image ddi2 on ddp2.id = ddi2.page_id
                    WHERE ddp2.source_id = dds.id AND ddi2.id IS NOT NULL)
                ) AS has_images,
                (SELECT string_agg(DISTINCT concat_ws('|', COALESCE(ddp.last_name, ''), COALESCE(ddp.first_name, ''),
                                                          COALESCE(ddp.earliest_year, -1), COALESCE(ddp.earliest_year_approximate, FALSE),
                                                          COALESCE(ddp.latest_year, -1), COALESCE(ddp.latest_year_approximate, FALSE), ddp.id), '$')
                     FROM diamm_data_item ddi
                              LEFT JOIN diamm_data_composition ddc on ddi.composition_id = ddc.id
                              LEFT JOIN diamm_data_compositioncomposer ddcc on ddc.id = ddcc.composition_id
                              LEFT JOIN diamm_data_person ddp ON ddcc.composer_id = ddp.id
                     WHERE ddi.source_id = dds.id AND ddp.id IS NOT NULL
                    ) AS composer_names,
                (SELECT string_agg(DISTINCT concat_ws('|', ddpi.identifier), '$')
                    FROM diamm_data_item ddi
                             LEFT JOIN diamm_data_composition ddc on ddi.composition_id = ddc.id
                             LEFT JOIN diamm_data_compositioncomposer ddcc on ddc.id = ddcc.composition_id
                             LEFT JOIN diamm_data_person ddp ON ddcc.composer_id = ddp.id
                             LEFT JOIN diamm_data_personidentifier ddpi ON ddp.id = ddpi.person_id
                    WHERE ddi.source_id = dds.id AND ddpi.identifier_type = 1 AND ddp.id IS NOT NULL
                    ) AS composer_ids,
                (SELECT string_agg(ddsn.note, '|:|')
                    FROM diamm_data_sourcenote ddsn
                    WHERE ddsn.source_id = dds.id AND ddsn.type = 1
                ) AS general_notes
            FROM diamm_data_source dds
                 LEFT JOIN diamm_data_archive dda on dds.archive_id = dda.id
                 LEFT JOIN diamm_data_archiveidentifier ddai ON dda.id = ddai.archive_id AND ddai.identifier_type = 1
                 LEFT JOIN diamm_data_geographicarea ddg on dda.city_id = ddg.id
                 LEFT JOIN diamm_data_sourceauthority ddsa ON ddsa.source_id = dds.id
            WHERE ddsa.source_id IS NULL OR ddsa.identifier_type != 1
            ORDER BY dds.id;""")

        while rows := curs.fetchmany(size=cfg["postgres"]["resultsize"]):
            yield rows


def _get_diamm_concordance(cfg: dict) -> Generator[list[dict[str, Any]], None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT DISTINCT dds.id AS id, ddsa.identifier AS rism_id,
                        dds.name AS name, dds.shelfmark AS shelfmark, dda.siglum AS siglum
                        FROM diamm_data_source dds
                        LEFT JOIN diamm_data_sourceauthority ddsa ON ddsa.source_id = dds.id
                        LEFT JOIN diamm_data_archive dda on dds.archive_id = dda.id
                        WHERE ddsa.source_id IS NOT NULL OR ddsa.identifier_type = 1
                        ORDER BY dds.id""")

        while rows := curs.fetchmany(size=cfg["postgres"]["resultsize"]):
            yield rows


def index_sources(cfg: dict) -> bool:
    source_groups = _get_sources(cfg)
    parallelise(source_groups, record_indexer, create_source_index_documents, cfg)

    diamm_sources = _get_diamm_concordance(cfg)
    parallelise(diamm_sources, update_source_records_with_diamm_info, cfg)

    return True


def update_source_records_with_diamm_info(sources: list, cfg: dict) -> bool:
    log.info("Updating RISM source records with DIAMM info")

    records = []

    for record in sources:
        label = f'{record.get("siglum", "")} {record.get("shelfmark", "")}'
        additional_fields = {}
        if n := record.get("name"):
            additional_fields["name"] = n

        doc = update_rism_document(
            record, "diamm", "source", label, cfg, additional_fields
        )
        if not doc:
            continue

        records.append(doc)

    check: bool = True if cfg["dry"] else submit_to_solr(records, cfg)

    if not check:
        log.error("There was an error submitting sources to Solr")

    return check
