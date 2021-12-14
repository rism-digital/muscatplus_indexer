from typing import Dict
import pysolr
import yaml
import argparse
import sys


if __name__ == "__main__":
    description: str = "Empties all records in a given solr core."
    parser = argparse.ArgumentParser()

    parser.add_argument("core", help="A solr core to empty. Should correspond to values in the silo_s field in Solr.")
    args = parser.parse_args()

    configuration: Dict = yaml.full_load(open('../index_config.yml', 'r'))  # nosec
    solr_server = configuration['solr']['server']
    core = args.core

    confirm = input(f"WARNING: This will completely empty the Solr core {core}. Type 'yes' to continue; Anything else will exit. >> ")  # nosec
    if confirm != "yes":
        sys.exit(0)
    else:
        connstring = f"{solr_server}/{core}"
        conn = pysolr.Solr(connstring, search_handler='iiif')
        conn.delete(q="*:*")
        conn.commit()
        print("All records have been deleted with \U00002764.")
