# Private soybean cargo and hedge workflow

Use the private offer comparison when two actual offers can supply the same soybean tonnage, destination, and loading window. The comparison calculates each offer's cost on a common delivered basis and passes the selected offer's stated pricing month and unpriced tonnes directly to the hedge calculator.

## Enter a case

Save a JSON object in `data/workspace/cargo/<case-id>.json`. Use letters, digits, underscores or hyphens for the case ID. The whole workspace is gitignored. Treat every case and result as desk data.

The following is an invented format example. Use the buyer's own offers, current timestamps, and cost inputs.

```json
{
  "decision_at": "2026-08-18T08:00:00-05:00",
  "destination": "cn_north",
  "quantity_mt": 25000,
  "loading_start": "2026-09-01",
  "loading_end": "2026-09-30",
  "offers": [
    {
      "id": "gulf",
      "origin": "us_gulf",
      "price_usd_mt": 420,
      "incoterm": "CIF",
      "carrier": "barge",
      "grade": "Sample soybean specification A",
      "observed_at": "2026-08-18T07:00:00-05:00",
      "expires_at": "2026-08-18T10:00:00-05:00",
      "loading_start": "2026-09-01",
      "loading_end": "2026-09-30",
      "available_mt": 25000,
      "unpriced_mt": 25000,
      "source": "Synthetic offer A",
      "contract": "ZSX26",
      "pricing_end": "2026-09-15",
      "duty_condition_confirmed": true
    },
    {
      "id": "brazil",
      "origin": "br_paranagua",
      "price_usd_mt": 458,
      "incoterm": "FOB",
      "carrier": "vessel",
      "grade": "Sample soybean specification B",
      "observed_at": "2026-08-18T07:00:00-05:00",
      "expires_at": "2026-08-18T10:00:00-05:00",
      "loading_start": "2026-09-01",
      "loading_end": "2026-09-30",
      "available_mt": 25000,
      "unpriced_mt": 25000,
      "source": "Synthetic offer B",
      "contract": "ZSX26",
      "pricing_end": "2026-09-15"
    }
  ]
}
```

`unpriced_mt` is the portion of the **requested** cargo still exposed to its board price. Set it to zero for a fully priced offer. Enter the physical offer's USD/MT value. You can add optional `basis_usd_mt` as part of the source record. The workflow retains it and uses the stated USD/MT price for the waterfall. The `contract` field carries the exact CBOT soybean contract. The hedge needs that reference and the date the physical price must be fixed. If a duty assumption carries a condition, confirm the applicable condition for this importer before setting `duty_condition_confirmed` to true.

Enter route costs through [the private assumptions workflow](../../data/reference/assumptions/private/README.md). Record the period and source for each cost. This includes any FOB elevation or port bridge, freight, insurance, discharge, financing, and quality adjustment. The waterfall shows which assumptions it used.

## Run and save

```bash
python scripts/cargo_desk.py <case-id>
python scripts/cargo_desk.py <case-id> --save <decision-id>
```

The first command prints the full decision to the invoking desk terminal. The second writes an owner-readable snapshot to `data/workspace/cargo/decisions/<decision-id>.json`. Use a new ID for each reprice. The saved snapshot includes the inputs, valuation result, applied assumption set ID, freight break-even, and hedge reference. These snapshots do not enter the public SQLite database or Pages build.

A ranking requires comparable totals for all entered offers. The cheapest offer is selected automatically when it is unique. Include a top-level `selected_offer` ID to choose an available comparable offer explicitly. The hedge uses a recent prior-session close for the named contract from the local market data. The engine blocks hedge sizing if the quote or exact reference is missing, or if the offer prices on or after first notice or expiry. The result is a sizing estimate for a purchaser and leaves freight, basis, FX, and quality exposures to be reviewed separately.

Run the synthetic full-path check with the existing fixture assumptions and an isolated market database.

```bash
python -m pytest -q --no-cov tests/test_cargo_desk.py::test_private_cargo_case_to_saved_named_hedge
```
