from typing import TypedDict, Optional

from indexer.helpers.utilities import clean_multivalued


class SubjectIndexDocument(TypedDict):
    id: str
    type: str
    term_s: str
    alternate_terms_sm: Optional[list[str]]
    notes_sm: Optional[list[str]]


def create_subject_index_document(subject: dict, cfg: dict) -> SubjectIndexDocument:
    d: SubjectIndexDocument = {
        "id": f"subject_{subject.get('id')}",
        "type": "subject",
        "term_s": subject["term"],
        "alternate_terms_sm": clean_multivalued(subject, "alternate_terms"),
        "notes_sm": clean_multivalued(subject, "notes")
    }

    return d

