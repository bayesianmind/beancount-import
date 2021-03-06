import collections
import logging
import sys
from typing import NamedTuple, Optional, List
import re
import os
import datetime
import csv

from beancount.core.amount import Amount
from beancount.core.data import Transaction, Posting, EMPTY_SET
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D, ZERO, Decimal, round_to
from beancount.core.position import Cost

from beancount_import.journal_editor import JournalEditor
from beancount_import.matching import FIXME_ACCOUNT
from beancount_import.source import SourceResults, description_based_source, \
    ImportResult
from beancount_import.source.description_based_source import \
    get_posting_source_descs
from beancount_import.unbook import group_postings_by_meta, unbook_postings

Basis = NamedTuple('Basis', [
    ('type', str),
    ('shares', Decimal),
    ('sale_price', Optional[Amount]),
    ('subscription_date', Optional[datetime.date]),
    ('subscription_fmv', Optional[Amount]),
    ('purchase_date', Optional[datetime.date]),
    ('purchase_price', Optional[Amount]),
    ('purchase_fmv', Optional[Amount]),
    ('grant_id', str),
    ('vest_date', datetime.date),
    ('vest_fmv', Amount),
    ('gross_proceeds', Optional[Amount]),
])

DepositShares = NamedTuple('DepositShares', [
    ('action', str),
    ('source_desc', str),
    ('file', str),
    ('date', datetime.date),
    ('symbol', str),
    ('description', str),
    ('shares', Decimal),
    ('award_date', datetime.date),
    ('award_id', str),
    ('vest_date', datetime.date),
    ('vest_fmv', Amount),
])

Sale = NamedTuple('Sale', [
    ('action', str),
    ('source_desc', str),
    ('file', str),
    ('date', datetime.date),
    ('symbol', str),
    ('description', str),
    ('shares', float),
    ('fees', Amount),
    ('disbursement_election', str),
    ('cash', Amount),
    ('basis', List[Basis])
])

JournalTransfer = NamedTuple('JournalTransfer', [
    ('action', str),
    ('source_desc', str),
    ('file', str),
    ('date', datetime.date),
    ('symbol', str),
    ('description', str),
    ('shares', float),
    ('cash', Optional[Amount]),
    ('basis', List[Basis]),
])


def dollars(s: str):
    if not s: s = "0"
    # I personally have only seen USD in these files, let me know if that's
    # an invalid assumption for Schwab EAC files.
    s = s.replace("$", "")
    return Amount(D(s), "USD")


def date(s: str):
    if not s: return None
    schwab_date_format = '%m/%d/%Y'
    if s.index("/") == 4:
        schwab_date_format = '%Y/%m/%d'
    return datetime.datetime.strptime(s, schwab_date_format).date()


def parse(filepath: str):
    entries = []

    header_re = re.compile(r'^[\"\w\&,]+$')
    normal_entry_re = re.compile(r'^\"\d\d\d\d/')
    with open(filepath, 'r') as file:
        file.readline()  # title
        header = file.readline()  # main header
        record_set = []
        line = next(file)
        # Schwab returns nested csv for some record types so we need a more
        # complex approach to reading the file.
        while True:
            if not line: line = next(file, None)
            if not line: break
            if not normal_entry_re.match(line):
                logging.fatal("invalid line:" + line)
            entry = next(csv.DictReader([header, line]))
            values = [v for v in entry.values() if v is not None]
            source_desc = ', '.join(values).strip(", ")
            line = None
            action = entry["Action"]
            if action == "Deposit":
                award_read = csv.DictReader([file.readline(), file.readline()])
                award = next(award_read)
                entries.append(DepositShares(
                    action=action,
                    source_desc=source_desc,
                    file=filepath,
                    date=date(entry["Date"]),
                    symbol=entry["Symbol"],
                    description=entry["Description"],
                    shares=D(entry["Quantity"]),
                    award_date=date(award["Award Date"]),
                    award_id=award["Award ID"],
                    vest_date=date(award["Vest Date"]),
                    vest_fmv=dollars(award["Vest FMV"]),
                ))
            elif action == "Transfer via Journal":
                # this entry has variable basis, so read until non-basis line
                # and then skip the normal next line reading
                basislines = []
                line = next(file, None)
                while line is not None:
                    if not line.startswith(r'"",'): break
                    basislines.append(line)
                    line = next(file, None)
                basisentries = []
                for basis in csv.DictReader(basislines):
                    basisentries.append(Basis(
                        type=basis["Type"],
                        shares=D(basis["Shares"]),
                        grant_id=basis["Grant Id"],
                        vest_date=date(basis["Vest Date"]),
                        vest_fmv=dollars(basis["Vest FMV"]),
                        sale_price=None,
                        subscription_date=None,
                        purchase_date=None,
                        purchase_price=None,
                        subscription_fmv=None,
                        purchase_fmv=None,
                        gross_proceeds=None,
                    ))
                entries.append(JournalTransfer(
                    action=action,
                    source_desc=source_desc,
                    file=filepath,
                    date=date(entry["Date"]),
                    symbol=entry["Symbol"],
                    description=entry["Description"],
                    shares=D(entry["Quantity"]),
                    cash=None,
                    basis=basisentries,
                ))
            elif action in ("Journal", "Wire Transfer", "Service Fee"):
                journal_entry = JournalTransfer(
                    action=action,
                    source_desc=source_desc,
                    file=filepath,
                    date=date(entry["Date"]),
                    symbol=entry["Symbol"],
                    description=entry["Description"],
                    shares=D(entry["Quantity"]),
                    cash=dollars(entry["Amount"]),
                    basis=[],
                )
                if journal_entry.shares == D("0") and \
                    journal_entry.cash.number == 0:
                    continue
                entries.append(journal_entry)
            elif action == "Sale":
                # this entry has variable basis, so read until non-basis line
                # and then skip the normal next line reading
                basislines = []
                line = next(file, None)
                while line is not None:
                    if not line.startswith(r'"",'): break
                    basislines.append(line)
                    line = next(file, None)
                basisentries = []
                for basis in csv.DictReader(basislines):
                    if D(basis["Shares"]) == 0: continue
                    basisentries.append(Basis(
                        type=basis["Type"],
                        shares=D(basis["Shares"]),
                        sale_price=dollars(basis["Sale Price"]),
                        subscription_date=date(basis["Subscription Date"]),
                        subscription_fmv=dollars(basis["Subscription FMV"]),
                        purchase_date=date(basis["Purchase Date"]),
                        purchase_price=dollars(basis["Purchase Price"]),
                        purchase_fmv=dollars(basis["Purchase FMV"]),
                        grant_id=basis["Grant Id"],
                        vest_date=date(basis["Vest Date"]),
                        vest_fmv=dollars(basis["Vest FMV"]),
                        gross_proceeds=dollars(basis["Gross Proceeds"]),
                    ))
                entries.append(Sale(
                    action=action,
                    source_desc=source_desc,
                    file=filepath,
                    date=date(entry["Date"]),
                    symbol=entry["Symbol"],
                    description=entry["Description"],
                    shares=D(entry["Quantity"]),
                    fees=dollars(entry["Fees & Commissions"]),
                    disbursement_election=entry["Disbursement Election"],
                    cash=dollars(entry["Amount"]),
                    basis=basisentries,
                ))
            else:
                logging.error("Unknown action type=" + action)
    return entries


def _get_key_from_posting(entry: Transaction, posting: Posting,
                          source_postings: List[Posting], source_desc: str,
                          posting_date: datetime.date):
    return (posting_date, source_desc)


def _get_key_from_entry(x):
    return (x.date, x.source_desc)


class SchwabEACSource(description_based_source.DescriptionBasedSource):
    def __init__(self,
                 directory: str,
                 cash_account: str,
                 eac_account: str,
                 stock_income_account: str,
                 fees_account: str,
                 pnl_account: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.cash_account = cash_account
        self.eac_account = eac_account
        self.stock_income_account = stock_income_account
        self.fees_account = fees_account
        self.pnl_account = pnl_account
        self.entries = []
        self.example_posting_key_extractors["source_desc"] = None

        for filename in os.listdir(directory):
            m = re.match(r'^EquityAwards.*.csv$', filename)
            if m is None:
                continue
            filepath = os.path.join(directory, filename)
            self.entries.extend(parse(filepath))

    def _make_import_result(self, x) -> ImportResult:
        if x._fields == DepositShares._fields:
            deposit = DepositShares(*x)
            cash_basis = Amount(
                number=round_to(deposit.vest_fmv.number * deposit.shares,
                                D("0.001")),
                currency=deposit.vest_fmv.currency)
            transaction = Transaction(
                meta=None,
                date=deposit.date,
                flag=FLAG_OKAY,
                payee=None,
                narration='Deposit %s as %d %s shares' %
                          (cash_basis, deposit.shares, deposit.symbol),
                tags=EMPTY_SET,
                links={deposit.award_id},
                postings=[
                    Posting(
                        account=self.eac_account,
                        units=Amount(deposit.shares, deposit.symbol),
                        cost=Cost(number=deposit.vest_fmv.number,
                                  currency=deposit.vest_fmv.currency,
                                  date=deposit.vest_date,
                                  label=None),
                        price=None,
                        flag=None,
                        meta=collections.OrderedDict(
                            source_desc=deposit.source_desc,
                            date=deposit.date,
                            award_date=deposit.award_date,
                        )),
                    Posting(
                        account=self.stock_income_account,
                        units=-cash_basis,
                        cost=None,
                        price=None,
                        flag=None,
                        meta=None,
                    ),
                ])
        elif x._fields == JournalTransfer._fields:
            tran = JournalTransfer(*x)
            dest_account = FIXME_ACCOUNT
            narration = tran.description
            if tran.symbol in self.commodity_dest_acct:
                dest_account = self.commodity_dest_acct[tran.symbol]
            if len(tran.basis) > 0:
                postings = []
                set_meta = False
                narration = f"{tran.shares} {tran.symbol} {narration}"
                for basis in tran.basis:
                    units = Amount(basis.shares, tran.symbol)
                    meta = collections.OrderedDict()
                    if not set_meta:
                        meta = collections.OrderedDict(
                            source_desc=tran.source_desc,
                            date=tran.date,
                        )
                        set_meta = True
                    postings.append(
                        Posting(
                            account=self.eac_account,
                            units=-units,
                            cost=Cost(basis.vest_fmv.number,
                                      basis.vest_fmv.currency,
                                      basis.vest_date,
                                      None),
                            price=None,
                            flag=None,
                            meta=meta
                        )
                    )
                    postings.append(
                        Posting(
                            account=dest_account,
                            units=units,
                            cost=Cost(basis.vest_fmv.number,
                                      basis.vest_fmv.currency,
                                      basis.vest_date,
                                      None),
                            price=None,
                            flag=None,
                            meta=None,
                        )
                    )
            else:
                postings = [
                    Posting(
                        account=self.eac_account,
                        units=tran.cash,
                        cost=None,
                        price=None,
                        flag=None,
                        meta=collections.OrderedDict(
                            source_desc=tran.source_desc,
                            date=tran.date,
                        )),
                    Posting(
                        account=dest_account,
                        units=-tran.cash,
                        cost=None,
                        price=None,
                        flag=None,
                        meta=None,
                    )
                ]
            if tran.action == "Journal":
                # TODO: infer correct account from description
                dest_account = self.cash_account
            transaction = Transaction(
                meta=None,
                date=tran.date,
                flag=FLAG_OKAY,
                payee=None,
                narration=narration,
                tags=EMPTY_SET,
                links=EMPTY_SET,
                postings=postings)

        elif x._fields == Sale._fields:
            sale = Sale(*x)
            transaction = Transaction(
                meta=None,
                date=sale.date,
                flag=FLAG_OKAY,
                payee=None,
                narration=sale.description + " %s %s" % (sale.shares,
                                                         sale.symbol),
                tags=EMPTY_SET,
                links=set(map(lambda b: b.grant_id, sale.basis)),
                postings=[
                    Posting(
                        account=self.eac_account,
                        units=sale.cash,
                        cost=None,
                        price=None,
                        flag=None,
                        meta=collections.OrderedDict(
                            source_desc=sale.source_desc,
                            date=sale.date,
                        )),
                    Posting(
                        account=self.fees_account,
                        units=sale.fees,
                        cost=None,
                        price=None,
                        flag=None,
                        meta=None,
                    ),
                ])
            for basis in sale.basis:
                isLong = sale.date - basis.vest_date > datetime.timedelta(365)
                costbasis = basis.vest_fmv.number * basis.shares
                gain = basis.gross_proceeds.number - costbasis
                transaction.postings.append(Posting(
                    account=self.eac_account,
                    units=Amount(-basis.shares, sale.symbol),
                    cost=Cost(
                        number=basis.vest_fmv.number,
                        currency=basis.vest_fmv.currency,
                        date=basis.vest_date,
                        label=None,
                    ),
                    price=basis.sale_price,
                    flag=None,
                    meta=collections.OrderedDict(
                        basis=costbasis,
                    ),
                ))
                # breaking gain out per basis makes it easier to file on taxes
                pnl_suffix = ":Long" if isLong else ":Short"
                transaction.postings.append(Posting(
                    account=self.pnl_account + pnl_suffix,
                    units=Amount(-gain, "USD"),
                    cost=None,
                    price=None,
                    flag=None,
                    meta=None,
                ))
        else:
            raise RuntimeError("Invalid import: " + x)
        return ImportResult(
            date=x.date,
            info=dict(type='text/csv', filename=x.file, line=1),
            entries=[transaction])

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        deduped_results = {}
        for entry in self.entries:
            deduped_results[_get_key_from_entry(entry)] = entry
        self.entries = deduped_results.values()
        self.dest_acct = {}
        self.commodity_dest_acct = {}
        for acct in journal.accounts.values():
            commodity_dest_key = "schwab_commodity_dest"
            if commodity_dest_key in acct.meta:
                commodity = acct.meta[commodity_dest_key]
                self.commodity_dest_acct[commodity] = acct.account
            journal_dest_key = "schwab_journal_name"
            if journal_dest_key in acct.meta:
                self.dest_acct[acct.meta[journal_dest_key]] = acct.account
        description_based_source.get_pending_and_invalid_entries(
            raw_entries=self.entries,
            journal_entries=journal.all_entries,
            # Only the subset of accounts where this source is authoritative.
            account_set={self.eac_account},
            get_key_from_posting=_get_key_from_posting,
            get_key_from_raw_entry=_get_key_from_entry,
            make_import_result=self._make_import_result,
            results=results,
        )

    def check_journal_for_duplicate_imports(self, journal: JournalEditor,
                                            results: SourceResults):
        seen_keys = {}
        for entry in journal.all_entries:
            if not isinstance(entry, Transaction):
                continue
            for postings in group_postings_by_meta(entry.postings):
                posting = unbook_postings(postings)
                if posting.meta is None:
                    continue
                if posting.account is not self.eac_account:
                    continue
                for source_desc, posting_date in get_posting_source_descs(
                    posting):
                    key = _get_key_from_posting(entry, posting, postings,
                                                source_desc, posting_date)
                    if key is None:
                        continue
                    meta = entry.meta
                    prev_location = seen_keys.get(key, None)
                    if prev_location is not None:
                        results.add_error(
                            f"Duplicate posting key at {meta.file}:{meta.line}"
                            "and at {meta.file}:{meta.line}"
                        )
                    seen_keys[key] = meta

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        return "source_desc" in posting.meta or "basis" in posting.meta

    @property
    def name(self):
        return 'schwab_eac'


def load(spec, log_status):
    return SchwabEACSource(log_status=log_status, **spec)


if __name__ == "__main__":
    for parsed_entry in parse(sys.argv[1]):
        print(parsed_entry)
        print("")
