import logging
import types
from typing import Callable, Dict, Any

import pymarc as pymarc
import ujson

from indexer.exceptions import RequiredFieldException
from indexer.helpers.utilities import to_solr_single_required, to_solr_multi, to_solr_multi_required, to_solr_single

log = logging.getLogger("muscat_indexer")


def process_marc_profile(cfg: Dict, doc_id: str, marc: pymarc.Record, processors: types.ModuleType) -> Dict:
    solr_document: Dict = {}

    for solr_field, field_config in cfg.items():
        multiple: bool = field_config.get("multiple", False)
        required: bool = field_config.get("required", False)

        if 'value' in field_config:
            # If we have a static value, simply set the field to the static value
            # and move on.
            solr_document[solr_field] = field_config['value']
        elif 'processor' in field_config:
            # a processor function is configured for this field.
            to_json = field_config.get("json", False)
            fn_name = field_config['processor']
            fn_exists = hasattr(processors, fn_name)

            if not fn_exists:
                log.error("Could not process Solr field %s for record %s; %s is a function that does not exist.", solr_field, doc_id, fn_name)
                continue

            processor_fn: Callable = getattr(processors, fn_name)
            field_result: Any = processor_fn(marc)

            if required is True and field_result is None:
                raise RequiredFieldException(f"{solr_field} requires a value, but one was not found for {doc_id}.")

            if field_result is None:
                # don't bother to add this to the result, since it would
                # get stripped out anyway.
                continue

            if to_json:
                field_result = ujson.dumps(field_result)

            solr_document[solr_field] = field_result
        else:
            ungrouped: bool = field_config.get("ungrouped", False)

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
            field_result = processor_fn(marc, marc_field, marc_subfield, ungrouped)
            if field_result is None:
                # For values of 'None' we would expect this field to not appear in the
                # document anyway, so we just skip any further processing or adding
                # this value to the result document.
                continue

            if 'value_prefix' in field_config:
                if isinstance(field_result, list):
                    prefixed_res_list = [f"{field_config['value_prefix']}_{v}" for v in field_result]
                    solr_document[solr_field] = prefixed_res_list
                elif isinstance(field_result, str):
                    prefixed_value = f"{field_config['value_prefix']}_{field_result}"
                    solr_document[solr_field] = prefixed_value
                else:
                    value_type = type(field_result)
                    log.error("A value prefix was configured for %s on %s, but %s cannot be prefixed!", solr_field, doc_id, value_type)
                    continue
            else:
                solr_document[solr_field] = field_result

    return solr_document
