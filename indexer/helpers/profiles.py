import logging
import types
from collections.abc import Callable

import orjson
import pymarc

from indexer.exceptions import RequiredFieldException
from indexer.helpers.utilities import (
    note_links,
    to_solr_multi,
    to_solr_multi_required,
    to_solr_single,
    to_solr_single_required,
)

log = logging.getLogger("muscat_indexer")


def process_marc_profile(
    cfg: dict, doc_id: str, marc: pymarc.Record, processors: types.ModuleType
) -> dict:
    solr_document: dict = {}

    for solr_field, field_config in cfg.items():
        multiple: bool = field_config.get("multiple", False)
        required: bool = field_config.get("required", False)
        to_json: bool = field_config.get("json", False)
        breaks: bool = field_config.get("breaks", False)
        links: bool = field_config.get("links", False)
        # Values are True, False, and None. Default is None.
        grouping: bool | None = field_config.get("grouping")
        sortout: bool = field_config.get("sorted", True)
        value_prefix = field_config.get("value_prefix")
        validator_fn_name: str | None = field_config.get("validator")
        value_from: str | None = field_config.get("value_from")
        processor: str | None = field_config.get("processor")

        if "value" in field_config:
            # If we have a static value, simply set the field to the static value
            # and move on.
            solr_document[solr_field] = field_config["value"]
            continue

        if value_from and not processor:
            if field_result := solr_document.get(value_from):
                solr_document[solr_field] = field_result
            else:
                log.error(
                    "The key %s is not in the Solr document, so the previously computed value is not available.",
                    value_from,
                )

            continue

        if processor:
            # a processor function is configured for this field.

            if not hasattr(processors, processor):
                log.error(
                    "Could not process Solr field %s for record %s; %s is a function that does not exist.",
                    solr_field,
                    doc_id,
                    processor,
                )
                continue

            processor_fn: Callable = getattr(processors, processor)

            if value_from:
                if input_value := solr_document.get(value_from):
                    log.debug(
                        "The key %s was previously computed and is now available.",
                        value_from,
                    )
                    field_result = processor_fn(input_value)
                else:
                    log.debug(
                        "The key %s is not in the Solr document, so the previously computed value is not available.",
                        value_from,
                    )
                    continue
            else:
                field_result = processor_fn(marc)

            if field_result is None:
                if required:
                    log.critical(
                        "%s requires a value, but one was not found for %s. Skipping this field.",
                        solr_field,
                        doc_id,
                    )
                continue

            # if validator_fn_name:
            #     validator_fn: Callable = getattr(processors, validator_fn_name)
            #     is_valid: bool = validator_fn(field_result, doc_id)
            #
            #     if not is_valid:
            #         log.warning("%s did not pass validation for %s on %s. It will not be included.", field_result, solr_field, doc_id)
            #         continue

            if to_json:
                field_result = orjson.dumps(field_result).decode("utf-8")

            solr_document[solr_field] = field_result
            continue

        # these will explode if the configuration is not correct.
        marc_field = field_config["field"]
        marc_subfield = field_config["subfield"]

        if multiple:
            processor_fn = to_solr_multi_required if required else to_solr_multi
        else:
            # not required and not multiple, default.
            processor_fn = to_solr_single_required if required else to_solr_single

        # This will raise an error if the processors encounter unexpected data.
        try:
            field_result = processor_fn(
                marc, marc_field, marc_subfield, grouping, sortout
            )
        except RequiredFieldException:
            log.critical(
                "%s requires a value, but one was not found for %s. Skipping this field.",
                solr_field,
                doc_id,
            )
            continue

        if field_result is None:
            # For values of 'None' we would expect this field to not appear in the
            # document anyway, so we just skip any further processing or adding
            # this value to the result document.
            continue

        # if validator_fn_name:
        #     validator_fn = getattr(processors, validator_fn_name)
        #     if multiple:
        #         is_valid = any(validator_fn(r) for r in field_result)
        #     else:
        #         is_valid = validator_fn(field_result, doc_id, marc_field, marc_subfield)
        #
        #     if not is_valid:
        #         log.error("\"%s\" did not pass validation for %s (%s $%s) on %s. It will not be included.", field_result, solr_field, marc_field, marc_subfield, doc_id)
        #         continue

        if multiple and breaks:
            # a field *must* be multivalued to support processing
            # breaks, since a break will create a list of values.
            field_result = [
                segment.strip()
                for res in field_result
                for segment in res.split("{{brk}}")
                if segment
            ]

        if links:
            if isinstance(field_result, list) and multiple:
                field_result = [note_links(res) for res in field_result]
            elif isinstance(field_result, str):
                field_result = note_links(field_result)

        if value_prefix:
            if isinstance(field_result, list):
                field_result = [f"{value_prefix}{v}" for v in field_result]
            elif isinstance(field_result, str):
                field_result = f"{value_prefix}{field_result}"
            else:
                log.warning(
                    "A value prefix was configured for %s on %s, but %s cannot be prefixed!",
                    solr_field,
                    doc_id,
                    type(field_result),
                )
                continue

        solr_document[solr_field] = field_result

    return solr_document
