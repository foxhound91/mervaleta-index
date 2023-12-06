import functions_framework
import pandas as pd
import yfinance as yf

from firestore_manager import insert_into_firestore

TICKERS = ["YPF", "GGAL", "PAM", "BMA", "ARCO", "TGS", "AGRO", "CEPU", "TEO",
           "CAAP", "BBAR", "LOMA", "EDN", "BIOX", "CRESY", "IRS", "SUPV"]
WEIGHTS = [13.63, 10.09, 9.67, 8.84, 7.95, 7.38, 7.26, 6.84, 6.37,
           4.05, 3.39, 2.88, 2.8, 2.59, 2.47, 2.19, 1.61]


class MissingTargetPriceException(Exception):
    """Exception raised when a target price is missing for a ticker."""

    def __init__(self, ticker, message="Target price is missing"):
        self.ticker = ticker
        self.message = message + f" for ticker: {ticker}"
        super().__init__(self.message)


def export_last_data(index_values_last_days, percent_variation, index_target, volatility):
    """
    Export today's data into file for record keeping
    """
    last_date = index_values_last_days.index[-1]
    last_index_value = index_values_last_days.iloc[-1]
    last_variation = percent_variation.iloc[-1]
    rec = recommendation(last_index_value, index_target, volatility)
    insert_into_firestore(last_date, last_index_value, last_variation, index_target, rec)


def elaborate_target(selected_tickers, tickers_weights):
    """
    Calculates and returns the composite target value of the index based on weighted analyst targets.
    Raises:
        MissingTargetPriceException: If the target price for any ticker is not found.
    Returns:
        float: The calculated composite index target value.
    """
    # Fetch analyst price targets and apply weights
    weighted_targets = []
    for ticker in selected_tickers:
        stock = yf.Ticker(ticker)
        target = stock.info.get('targetMeanPrice')
        if target:
            weighted_target = target * tickers_weights[ticker]
            weighted_targets.append(weighted_target)
        else:
            raise MissingTargetPriceException(ticker)

    # Calculate the index target
    _index_target = sum(weighted_targets)
    print(f"\nComposite Index Target: {_index_target:.2f}")
    return _index_target


def recommendation(index_price, index_target, volatility):
    """
    Prints a recommendation (BUY, SELL, HOLD) based on the index's current price, target, and volatility.
    """
    # Define the volatility threshold using the existing volatility percentage
    volatility_threshold = index_price * volatility / 100

    # Compare the index_target with the index price and print the recommendation
    if index_target > index_price + volatility_threshold:
        return "BUY"
    elif index_target < index_price - volatility_threshold:
        return "SELL"
    else:
        return "HOLD"


def check_top_tickers(downloaded_tickers):
    """
    Determines and prints the top 5 best and worst performing stocks in the index.
    """
    # Calculate the total percentage change for each ticker
    start_prices = downloaded_tickers.loc[downloaded_tickers.first_valid_index()]
    latest_prices = downloaded_tickers.iloc[-1]
    total_pct_change = (latest_prices - start_prices) / start_prices * 100

    # Sort the percentage changes to find top and bottom performers
    sorted_pct_change = total_pct_change.sort_values()

    print("\nTop 5 Best Performers:")
    print('\n'.join(sorted_pct_change[-5:][::-1].apply(lambda x: f"{x:.2f}%").to_string(index=True).split('\n')))

    print("\nTop 5 Worst Performers:")
    print('\n'.join(sorted_pct_change[:5].apply(lambda x: f"{x:.2f}%").to_string(index=True).split('\n')))


@functions_framework.http
def mervaleta_index(request):
    weights_df = pd.Series(WEIGHTS, index=TICKERS) / 100

    # Downloading the closing prices
    df = yf.download(TICKERS, start='2023-11-20', auto_adjust=True, progress=False)['Close']

    # Calculate the index value for the last X days
    # Determined by a weighted sum of the closing prices of the constituent stocks.
    # Each stock's closing price is multiplied by its assigned weight
    # and these products are then summed up to get the total index value.
    index_values_last_days = df.apply(lambda x: (x * weights_df).sum(), axis=1)

    # Calculate the percentage variation compared to the previous day
    percent_variation = index_values_last_days.pct_change() * 100

    # Printing the index price and the percentage variation on the same line
    for date, (index_value, variation) in zip(index_values_last_days.index,
                                              zip(index_values_last_days, percent_variation)):
        print(f"{date.date()} Index Price: {index_value:.2f}, Variation: {variation:.2f}%")

    # Calculate the standard deviation (volatility)
    volatility = percent_variation.std()
    print(f"\nVolatility of the Index: {volatility:.2f}%")

    try:
        index_target = elaborate_target(TICKERS, weights_df)
    except MissingTargetPriceException as e:
        return f'ERROR - {e}'

    print("Recommendation: " + recommendation(index_values_last_days.iloc[-1], index_target, volatility))

    check_top_tickers(df)

    export_last_data(index_values_last_days, percent_variation, index_target, volatility)
    return 'finished successfully'
