import re
from typing import Optional

RISM_ID_SUB: re.Pattern = re.compile(r"(?:people|sources|institutions)\/(?P<doc_id>\d+)")


def transform_rism_id(q_id: Optional[str]) -> Optional[str]:
    """
    Transform an incoming RISM ID into a Solr ID.
    :param q_id: Query ID
    :return: A Solr ID string, or None if not successful.
    """
    if not q_id:
        return None

    doc_matcher: re.Match = re.match(RISM_ID_SUB, q_id)
    if not doc_matcher:
        return None

    doc_num: str = doc_matcher["doc_id"]
    if "people" in q_id:
        return f"person_{doc_num}"
    elif "sources" in q_id:
        return f"source_{doc_num}"
    elif "institutions" in q_id:
        return f"institution_{doc_num}"
    else:
        return None
