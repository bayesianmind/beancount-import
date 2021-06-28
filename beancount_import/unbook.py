"""Facilities for reversing the booking process to recover the original posting."""

from typing import Iterable, List, Optional
from beancount.core.data import Posting, CostSpec, Meta, Amount
from beancount.core.number import ZERO, MISSING

def group_postings_by_meta(postings: Iterable[Posting]) -> Iterable[List[Posting]]:
    """Groups postings that have identical, non-None meta values together.

    This is intended to group together multiple postings generated by the
    booking procedure that correspond to a single original posting.
    """
    prev_meta = None # type: Optional[Meta]
    posting_list = []
    for posting in postings:
        if posting.meta is not None and (posting.meta is prev_meta or
           posting.meta.get("merge")):
            posting_list.append(posting)
            continue
        if posting_list:
            yield posting_list
        prev_meta = posting.meta
        posting_list = [posting]
    if posting_list:
        yield posting_list


def unbook_postings(postings: List[Posting]) -> Posting:
    """Unbooks a list of postings back into a single posting.

    The combined units are computed, the cost and price are left unspecified.
    """
    if len(postings) == 1:
        return postings[0]
    number = sum((posting.units.number for posting in postings), ZERO)
    return postings[0]._replace(
        units=Amount(number=number, currency=postings[0].units.currency),
        cost=CostSpec(
            number_per=None,
            number_total=None,
            currency=None,
            date=None,
            label=None,
            merge=None))
