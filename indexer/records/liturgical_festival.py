from typing import TypedDict, Optional, List, Dict

from indexer.helpers.utilities import clean_multivalued


class LiturgicalFestivalIndexDocument(TypedDict):
    id: str
    type: str
    name_s: str
    alternate_terms_sm: Optional[List[str]]
    notes_sm: Optional[List[str]]


def create_liturgical_festival_document(festival: Dict) -> LiturgicalFestivalIndexDocument:
    d: LiturgicalFestivalIndexDocument = {
        "id": f"festival_{festival.get('id')}",
        "type": "liturgical_festival",
        "name_s": f"{festival.get('name')}",
        "alternate_terms_sm": clean_multivalued(festival, "alternate_terms"),
        "notes_sm": clean_multivalued(festival, "notes")
    }

    return d
