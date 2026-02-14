"""
Crush spread calculation for soybeans.

The crush spread measures how profitable it is to "crush" (process)
soybeans into soybean oil and soybean meal.

Key concepts for learning:
    - Soybeans are crushed into two products: oil (cooking, biodiesel) and meal (animal feed)
    - The crush margin = value of products - cost of raw soybeans
    - Positive margin = profitable to crush → processors buy more beans → supports prices
    - Negative margin = losses → processors slow down → less demand for beans
    - This is one of the most important relationships in agricultural commodities
"""

import pandas as pd


def compute_crush_spread(
    soybeans_df: pd.DataFrame,
    oil_df: pd.DataFrame,
    meal_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute daily crush margin from CBOT futures prices.

    Formula (CBOT board crush, all in cents per bushel):
        Crush Spread = (Soybean Oil price x 11) + (Soybean Meal price x 2.2) - Soybean price

    Where:
        - Oil price is in cents/lb; 1 bushel → ~11 lbs oil → multiply by 11
        - Meal price is in $/short ton; 1 bushel → ~0.022 short tons meal
          but we multiply by 100 to convert $ to cents → 0.022 x 100 = 2.2
        - Soybean price is already in cents/bushel

    Parameters
    ----------
    soybeans_df : pd.DataFrame
        Soybean prices with 'Close' column and Date index.
    oil_df : pd.DataFrame
        Soybean oil prices with 'Close' column and Date index.
    meal_df : pd.DataFrame
        Soybean meal prices with 'Close' column and Date index.

    Returns
    -------
    pd.DataFrame
        Columns: Date, crush_spread, soybeans_close, oil_close, meal_close
    """
    # Align all three on the same dates using inner join
    combined = pd.DataFrame({
        "soybeans_close": soybeans_df["Close"],
        "oil_close":      oil_df["Close"],
        "meal_close":     meal_df["Close"],
    }).dropna()

    combined["crush_spread"] = (
        combined["oil_close"] * 11
        + combined["meal_close"] * 2.2
        - combined["soybeans_close"]
    )

    result = combined.reset_index()
    result.columns = ["Date", "soybeans_close", "oil_close", "meal_close", "crush_spread"]

    return result
