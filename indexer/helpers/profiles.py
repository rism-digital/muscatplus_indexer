import logging
import types
from typing import Callable, Any, Optional

import pymarc
import orjson

from indexer.exceptions import RequiredFieldException
from indexer.helpers.utilities import (
    to_solr_single_required,
    to_solr_multi,
    to_solr_multi_required,
    to_solr_single,
    note_links
)

log = logging.getLogger("muscat_indexer")


def process_marc_profile(cfg: dict, doc_id: str, marc: pymarc.Record, processors: types.ModuleType) -> dict:
    solr_document: dict = {}

    for solr_field, field_config in cfg.items():
        multiple: bool = field_config.get("multiple", False)
        required: bool = field_config.get("required", False)

        if 'value' in field_config:
            # If we have a static value, simply set the field to the static value
            # and move on.
            solr_document[solr_field] = field_config['value']
        elif 'processor' in field_config:
            # a processor function is configured for this field.
            to_json: bool = field_config.get("json", False)
            fn_name: str = field_config['processor']
            fn_exists: bool = hasattr(processors, fn_name)

            if not fn_exists:
                log.warning("Could not process Solr field %s for record %s; %s is a function that does not exist.",
                            solr_field, doc_id, fn_name)
                continue

            processor_fn: Callable = getattr(processors, fn_name)
            field_result: Any = processor_fn(marc)

            if required is True and field_result is None:
                log.critical("%s requires a value, but one was not found for %s. Skipping this field.",
                             solr_field, doc_id)
                continue

            if field_result is None:
                # don't bother to add this to the result, since it would
                # get stripped out anyway.
                continue

            if to_json:
                field_result: str = orjson.dumps(field_result).decode("utf-8")

            solr_document[solr_field] = field_result
        else:
            breaks: bool = field_config.get("breaks", False)
            links: bool = field_config.get("links", False)
            # Values are True, False, and None. Default is None.
            grouping: Optional[bool] = field_config.get("grouping")
            sortout: bool = field_config.get("sorted", True)

            # these will explode if the configuration is not correct.
            marc_field = field_config['field']
            marc_subfield = field_config['subfield']

            processor_fn: Callable
            if required and multiple:
                processor_fn = to_solr_multi_required
            elif not required and multiple:
                processor_fn = to_solr_multi
            elif required and not multiple:
                processor_fn = to_solr_single_required
            else:
                # not required and not multiple, default.
                processor_fn = to_solr_single

            # This will raise an error if the processors encounter unexpected data.
            try:
                field_result = processor_fn(marc, marc_field, marc_subfield, grouping, sortout)
            except RequiredFieldException:
                log.critical("%s requires a value, but one was not found for %s. Skipping this field.",
                             solr_field, doc_id)
                continue

            if field_result is None:
                # For values of 'None' we would expect this field to not appear in the
                # document anyway, so we just skip any further processing or adding
                # this value to the result document.
                continue

            if multiple and breaks:
                # a field *must* be multivalued to support processing
                # breaks, since a break will create a list of values.
                full_result = []
                for res in field_result:
                    m = [s.strip() for s in res.split("{{brk}}") if s]
                    full_result += m
                # set the field result to the new values from the processed
                # breaks.
                field_result = full_result

            if multiple and links:
                link_result: list = []
                for res in field_result:
                    linked = note_links(res)
                    link_result.append(linked)
                field_result = link_result
            elif multiple is False and links:
                field_result = note_links(field_result)

            if 'value_prefix' in field_config:
                if isinstance(field_result, list):
                    prefixed_res_list = [f"{field_config['value_prefix']}{v}" for v in field_result]
                    solr_document[solr_field] = prefixed_res_list
                elif isinstance(field_result, str):
                    prefixed_value = f"{field_config['value_prefix']}{field_result}"
                    solr_document[solr_field] = prefixed_value
                else:
                    value_type = type(field_result)
                    log.warning("A value prefix was configured for %s on %s, but %s cannot be prefixed!",
                                solr_field, doc_id, value_type)
                    continue
            else:
                solr_document[solr_field] = field_result

    return solr_document
