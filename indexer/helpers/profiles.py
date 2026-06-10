import logging
import types
from collections.abc import Callable
from dataclasses import dataclass

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


@dataclass(slots=True)
class CompiledProfileField:
    solr_field: str
    multiple: bool
    required: bool
    to_json: bool
    breaks: bool
    links: bool
    grouping: bool | None
    sortout: bool
    value_prefix: str | None
    value_from: str | None
    additional_data: dict | None
    static_value: object | None = None
    processor_fn: Callable | None = None
    marc_extractor: Callable | None = None
    marc_field: str | None = None
    marc_subfield: str | None = None


def compile_marc_profile(
    cfg: dict,
    processors: types.ModuleType,
) -> list[CompiledProfileField]:
    compiled: list[CompiledProfileField] = []

    for solr_field, field_config in cfg.items():
        multiple: bool = field_config.get("multiple", False)
        required: bool = field_config.get("required", False)
        processor_name: str | None = field_config.get("processor")
        static_value = field_config.get("value")
        value_from: str | None = field_config.get("value_from")

        processor_fn: Callable | None = None
        if processor_name:
            if not hasattr(processors, processor_name):
                log.error(
                    "Could not compile Solr field %s; %s is a function that does not exist.",
                    solr_field,
                    processor_name,
                )
            else:
                processor_fn = getattr(processors, processor_name)

        marc_extractor: Callable | None = None
        marc_field: str | None = None
        marc_subfield: str | None = None
        if static_value is None and value_from is None and processor_name is None:
            marc_field = field_config["field"]
            marc_subfield = field_config["subfield"]
            if multiple:
                marc_extractor = (
                    to_solr_multi_required if required else to_solr_multi
                )
            else:
                marc_extractor = (
                    to_solr_single_required if required else to_solr_single
                )

        compiled.append(
            CompiledProfileField(
                solr_field=solr_field,
                multiple=multiple,
                required=required,
                to_json=field_config.get("json", False),
                breaks=field_config.get("breaks", False),
                links=field_config.get("links", False),
                grouping=field_config.get("grouping"),
                sortout=field_config.get("sorted", True),
                value_prefix=field_config.get("value_prefix"),
                value_from=value_from,
                additional_data=field_config.get("additional_data"),
                static_value=static_value,
                processor_fn=processor_fn,
                marc_extractor=marc_extractor,
                marc_field=marc_field,
                marc_subfield=marc_subfield,
            )
        )

    return compiled


def process_marc_profile(
    cfg: list[CompiledProfileField],
    doc_id: str,
    marc: pymarc.Record,
    dbdata: dict | None = None,
) -> dict:
    solr_document: dict = {}

    for field in cfg:
        solr_field = field.solr_field

        if field.static_value is not None:
            # If we have a static value, simply set the field to the static value
            # and move on.
            solr_document[solr_field] = field.static_value
            continue

        if field.value_from and field.processor_fn is None:
            if field_result := solr_document.get(field.value_from):
                solr_document[solr_field] = field_result
            else:
                log.error(
                    "The key %s is not in the Solr document, so the previously computed value is not available.",
                    field.value_from,
                )

            continue

        if field.processor_fn:
            kwargs: dict = {}
            if dbdata is not None and field.additional_data is not None:
                addn = {k: dbdata[r] for k, r in field.additional_data.items()}
                kwargs.update(addn)

            if field.value_from:
                if input_value := solr_document.get(field.value_from):
                    log.debug(
                        "The key %s was previously computed and is now available.",
                        field.value_from,
                    )
                    field_result = field.processor_fn(input_value, **kwargs)
                else:
                    log.debug(
                        "The key %s is not in the Solr document, so the previously computed value is not available.",
                        field.value_from,
                    )
                    continue
            else:
                field_result = field.processor_fn(marc, **kwargs)

            if field_result is None:
                if field.required:
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

            if field.to_json:
                field_result = orjson.dumps(field_result).decode("utf-8")

            solr_document[solr_field] = field_result
            continue

        if field.marc_extractor is None:
            continue

        # This will raise an error if the processors encounter unexpected data.
        try:
            field_result = field.marc_extractor(
                marc,
                field.marc_field,
                field.marc_subfield,
                field.grouping,
                field.sortout,
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

        if isinstance(field_result, list) and field.multiple:
            if field.breaks or field.links or field.value_prefix:
                processed_values: list[str] = []

                for res in field_result:
                    segments = res.split("{{brk}}") if field.breaks else [res]
                    for segment in segments:
                        if field.breaks:
                            # a field *must* be multivalued to support processing
                            # breaks, since a break will create a list of values.
                            if not segment:
                                continue
                            segment = segment.strip()

                        if field.links:
                            segment = note_links(segment)

                        if field.value_prefix:
                            segment = f"{field.value_prefix}{segment}"

                        processed_values.append(segment)

                field_result = processed_values

        elif field.links and isinstance(field_result, str):
            field_result = note_links(field_result)

        if field.value_prefix and not isinstance(field_result, (list, str)):
            log.warning(
                "A value prefix was configured for %s on %s, but %s cannot be prefixed!",
                solr_field,
                doc_id,
                type(field_result),
            )
            continue

        if field.value_prefix and isinstance(field_result, str):
            field_result = f"{field.value_prefix}{field_result}"

        solr_document[solr_field] = field_result

    return solr_document
