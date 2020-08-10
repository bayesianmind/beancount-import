"""Plaid.com transaction source.

This imports transactions from Plaid JSON export files.

Data format
===========

To use, export transaction data in a JSON array with one object per transaction.
This importer looks for transaction and balance files.

You might have a directory structure like:

    financial/
      data/
        plaid/
          transactions_<start>_<end>.json
          balances_<date>.json

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Plaid source:

    dict(module='beancount_import.source.plaid',
         directory=os.path.join(journal_dir, 'data', 'plaid', 'plaid.csv'),
         balances_directory=os.path.join(journal_dir, 'data', 'plaid'),
    )

where `journal_dir` refers to the financial/ directory.  Specifying the
`balances_directory` key is optional.  If not specified, balance information
won't be imported.

Associating Plaid accounts with Beancount accounts
=================================================

This data source only imports transactions from accounts known to Plaid with
which a Beancount account has been explicitly associated using the `plaid_id`
metadata field of the account open directive.  The `plaid_id` corresponds to the
"Account Name" field in the CSV file.  As this "Account Name" excludes the
institution name, it is possible that the "Account Name" values are not unique,
in which case you can change them using the Plaid.com web interface, before
re-downloading the transactions.  For example:

    1900-01-01 open Liabilities:Credit-Card  USD
      plaid_id: "My Credit Card"

    1900-01-01 open Assets:Checking  USD
      plaid_id: "My Checking"

    1900-01-01 open Liabilities:Amazon-Store-Card  USD
      plaid_id: "Amazon Store Card"

Imported transaction format:
============================

Each row in the transactions CSV file corresponds to a single imported
transaction of the form:

    2016-08-10 * "STARBUCKS STORE 12345"
      Liabilities:Credit-Card  -2.45 USD
        date: 2016-08-10
        source_desc: "STARBUCKS STORE 12345"
      Expenses:FIXME            2.45 USD

Transaction identification
--------------------------

The `date` and `source_desc` metadata fields (along with the account and amount)
associate postings in the journal with corresponding rows in the transactions
CSV file.  These fields correspond to the "Date" and "Original Description"
fields in the transactions CSV file, respectively.  It is possible for multiple
real transactions to have an identical combination of account, amount, "Date",
and "Original Description" (corresponding to multiple identical rows in the
transactions CSV file), but that is handled appropriately: this data source will
simply generate a separate transaction for each such row.

The transactions CSV export format provided by Plaid and consumed by this data
source does not include a unique transaction identifier, except in the case that
Plaid has (erroneously) included a unique identifier provided by the financial
institution in the "Original Description" field.  Internally, Plaid does expose a
unique transaction identifier through the undocumented JSON API, but this data
source does not attempt to use them.

Unknown account prediction
--------------------------

The `source_desc` metadata field provides features for predicting the unknown
account.  The transactions CSV format includes additional "Description" and
"Category" fields that are synthesized by Plaid from the original data, and
potentially provide some information that could be useful for predicting the
unknown account.  However, this data source does not rely on those fields, as
they are not stable (meaning they may change on a subsequent download).
"""

from typing import List, Union, Optional, Set, Dict
import json
import datetime
import collections
import re
import os

from beancount.core.data import Transaction, Posting, Balance, EMPTY_SET, \
    Directive, Meta
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D, ZERO

from . import ImportResult, Source, SourceResults, description_based_source
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

METADATA_ACCT_ID = "plaid_account_id"
METADATA_TRAN_ID = "plaid_transaction_id"

def load_transactions(filename: str) -> List[Dict]:
    try:
        match = re.search(r"(\d\d\d\d-\d\d-\d\d).bal", filename)
        with open(filename, 'r', encoding='utf-8', newline='') as jsonfile:
            entries = json.load(jsonfile)
            for entry in entries:
                entry['file'] = filename
                if match:
                    entry['date'] = entry.setdefault('date', match.group(1))
        return entries

    except Exception as e:
        raise RuntimeError('JSON file is invalid', filename) from e


def _get_entry_transaction_id(entry: Directive):
    if not isinstance(entry, Transaction): return []
    transaction_ids = []
    for posting in entry.postings:
        meta = posting.meta
        if meta is None: continue
        transaction_id = meta.get(METADATA_TRAN_ID)
        if transaction_id:
            transaction_ids.append(transaction_id)
    return transaction_ids


def get_transaction_ids_seen(journal: JournalEditor) -> Set[str]:
    transaction_ids = set()
    for entry in journal.all_entries:
        for transaction_id in  _get_entry_transaction_id(entry):
            transaction_ids.add(transaction_id)
    return transaction_ids


def _make_import_result(entry) -> ImportResult:
    tags = EMPTY_SET
    date = datetime.date.fromisoformat(entry['date'])
    if 'transaction_id' in entry:
        meta = collections.OrderedDict(
            date=date,
            plaid_transaction_id=entry['transaction_id'],
        )
        if entry['account_owner']:
            meta["account_owner"] = entry['account_owner']
        if entry['category']:
            meta["category"] = ", ".join(entry['category'])
        if not(entry['pending'] and bool(entry['pending'])):
            meta["source_desc"] = entry['name']
        sign = -1
        amount = Amount(number=sign * round(D(entry['amount']), 2),
                        currency=entry['iso_currency_code']
                        )
        journal_entry = Transaction(
            meta=None,
            date=date,
            flag=FLAG_OKAY,
            payee=entry['merchant_name'],
            narration=entry['name'],
            tags=tags,
            links=EMPTY_SET,
            postings=[
                Posting(
                    account=entry['account'],
                    units=amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=meta,
                ),
                Posting(
                    account=FIXME_ACCOUNT,
                    units=-amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=None,
                ),
            ])
    else:
        balance = entry['balances']
        journal_entry = Balance(
            date=date,
            meta=None,
            account=entry['account'],
            amount=Amount(
                number=D(balance['current']),
                currency=balance['iso_currency_code'],
            ),
            tolerance=None,
            diff_amount=None,
        )
    return ImportResult(
        date=date,
        info=dict(
            type='text/plain',
            filename=entry['file'],
        ),
        entries=[journal_entry],
    )


class PlaidSource(Source):
    def __init__(self,
                 directory: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.filenames = directory

        self.plaid_entries = []
        for file in os.listdir(directory):
            if not file.endswith(".txt"):
                continue
            file = os.path.abspath(os.path.join(directory, file))
            self.log_status('plaid: loading %s' % file)
            self.plaid_entries.extend(load_transactions(file))

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        account_to_plaid_id, plaid_id_to_account = \
            description_based_source.get_account_mapping(
            journal.accounts, METADATA_ACCT_ID)
        missing_accounts = set()  # type: Set[str]
        seen_trans_ids = get_transaction_ids_seen(journal)

        for entry in self.plaid_entries:
            if entry.get('transaction_id') in seen_trans_ids:
                continue
            account_id = plaid_id_to_account.get(entry['account_id'])
            if not account_id:
                missing_accounts.add(entry['account_id'])
                continue
            entry["account"] = account_id
            results.add_accounts(account_to_plaid_id.keys())
            results.add_pending_entry(_make_import_result(entry))

        for plaid_account in missing_accounts:
            results.add_warning(
                'No Beancount account with plaid_account_id: %r.' %
                (plaid_account, ))

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        return METADATA_TRAN_ID in posting.meta

    def get_example_key_value_pairs(self, transaction: Transaction,
                                    posting: Posting):
        key_values = {}
        for key in ("category", "account_owner", "source_desc"):
            val = posting.meta.get(key)
            if val is not None:
                key_values[key] = val
        return key_values

    @property
    def name(self):
        return 'plaid'


def load(spec, log_status):
    return PlaidSource(log_status=log_status, **spec)
