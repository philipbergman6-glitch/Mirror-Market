# Verification records

One JSON line per checked number, written by the verifier (`verify/`, spec #639)
or, for the human CME sitting, through a reviewed PR. This directory is the
accuracy ledger's only input. It is **not** `data/history/`: the history-guard
never sees it.

- **Layout:** `<YYYY-MM-DD of the check>/<verified_at>-<check>.jsonl`. A file is
  created once and never rewritten; a later run writes a new file.
- **Schema:** `verify/record.py` → `Record`, the spec's field list in order, plus
  `publisher` and `withheld_reason`. `null` means not learned or not
  applicable, never zero.
- **Never re-graded:** each record stamps the `tolerance_version` it was graded
  under. A tolerance change grades only records written after it.
- **Licence-safe:** a value the verifying publisher's terms forbid republishing,
  and the displayed value of a licence-restricted series (CBOT, DCE, CEPEA,
  AgRural, SAFEX), is never written here. The deltas, the result and the
  `withheld_reason` always are (`verify/licence.py`).
