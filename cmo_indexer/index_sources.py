from pathlib import Path
from pymarc.marcxml import parse_xml_to_array

from cmo_indexer.records.source import create_source_index_document


def index_sources(cfg: dict) -> bool:
    sources_marc_files_dir: Path = Path(cfg['cmo']['sources_marc'])
    expressions_marc_files_dir: Path = Path(cfg['cmo']['expressions_marc'])

    source_contents = {}
    for source_file in sources_marc_files_dir.iterdir():
        source_ident = source_file.stem
        source_contents[source_ident] = parse_xml_to_array(source_file)[0]

    expression_contents = {}
    for expression_file in expressions_marc_files_dir.iterdir():
        expression_ident = expression_file.stem
        expression_contents[expression_ident] = parse_xml_to_array(expression_file)[0]

    for source_ident, parsed_source in source_contents.items():
        solr_records = create_source_index_document(parsed_source, expression_contents, cfg)

    return True
