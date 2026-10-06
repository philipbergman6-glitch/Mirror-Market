# PROTOTYPE — P1 #362 (map #356). Throwaway branch, never merged.

Three presentation variants of footprint weather and cyclone hazard flags, mounted on
snapshots of the live CBOT and Brazil market pages (taken 2026-10-06), using K2 #361's
real numbers (ECMWF IFS 00z 2026-10-05, ERA5 1991–2020 normal, SPAM-weighted admin-1).

Run:  python3 -m http.server 8356 -d prototype/p1-footprint-presentation
Open: http://localhost:8356/markets/cbot.html?variant=A&scenario=today

- `?variant=A|B|C`: A = cards in place, B = scan table, C = story first
- `?scenario=today|francine`: Francine = NHC al062024 advisory 010, replayed as if today
- Keys: ← → cycle the variants, S flips the scenario. The bottom bar does the same.
- Hatched cards are footprints the spike did not compute. They are placeholders, not numbers.
