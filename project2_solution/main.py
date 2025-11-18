"""
Main module for Project 2

This module includes utilities to run and test the functions in the
`task_project2` module.

IMPORTANT:
Please refer to the project description for further details about this module.
"""
import pandas as pd

from projects.project2.helpers import (
        fmt_dt,
        fmt_tic,
        read_ma_deals,
        read_org_ff,
        read_stk_rets,
        summarise_series,
        wide_to_long_rets,
        print_msg,
        )

from projects.project2.task_project2 import (
        mk_ma_info,
        mk_stk_arets,
        mean_by_dates,
        expand_event_dates,
        mk_buy_tgt_sell_acq_rets,
        mk_buy_tgt_sell_mkt_rets,
        mk_tgt_rets_by_event_time,
        mk_prop_positive_tgt_rets,
        )


def main():
    """
    Run the full project workflow for analysing the profitability of
    trading strategies around M&A announcements.

    This function performs the following steps:

    1. **Load the input data**
       - Daily stock returns (`ma_rets.csv`)
       - M&A deal information (`ma_deals.csv`)
       - Fama–French factors (`FF_Research_Data_Factors_daily.csv`)

    2. **Construct deal-level information**
       Using `mk_ma_info`, the function organises the deal data into a
       cleaner format with one row per deal and columns identifying the
       acquirer, target, announcement date, and deal number.

    3. **Compute abnormal returns**
       Using `mk_stk_arets`, the function computes abnormal returns for
       each stock and date, defined as the stock return minus the market
       return.

    4. **Expand events across event windows**
       Using `expand_event_dates`, the function creates a table where each
       deal is repeated once for every valid return date from 1 to 30
       days after the announcement.

    5. **Construct trading strategy returns**
       - `mk_buy_tgt_sell_acq_rets` computes daily portfolio returns for a
         strategy that buys the target and sells the acquirer.
       - `mk_buy_tgt_sell_mkt_rets` computes daily portfolio returns for a
         strategy that buys the target and sells the market (using
         abnormal returns).

    6. **Build an event-time panel for target returns**
       Using `mk_tgt_rets_by_event_time`, the function converts target
       returns into a deal-by-event-time matrix.

    7. **Compute event-time positive-return proportions**
       Using `mk_prop_positive_tgt_rets`, the function calculates, for each
       event time, the proportion of deals with a strictly positive target
       return.

    Notes
    -----

    This function will not display any information. See `_test_main` for a
    version of this function which displays information about the
    profitability of trading strategies

    """
    print_msg("Running main...", as_header=True)

    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns 
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
     )

    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )

    # 5: Construct trading strategy returns

    # from buying the target and selling the acquirer
    buy_tgt_sell_acq_rets = mk_buy_tgt_sell_acq_rets(
            expanded_ma_info=expanded_ma_info,
            stk_rets=stk_rets,
            )

    # from buying the target and selling the market
    buy_tgt_sell_mkt_rets = mk_buy_tgt_sell_mkt_rets(
            expanded_ma_info=expanded_ma_info,
            stk_arets=stk_arets,
            )


    # 6: Build an event-time panel for target returns
    tgt_rets_by_event_time = mk_tgt_rets_by_event_time(
        stk_rets=stk_rets,
        expanded_ma_info=expanded_ma_info,
        )

    # 7: Compute event-time positive-return proportions
    prop_positive_tgt_rets = mk_prop_positive_tgt_rets(
        tgt_rets_by_event_time= tgt_rets_by_event_time)


# ----------------------------------------------------------------------------
#  Test functions
#
#  IMPORTANT: If a function is named "test_..." instead of "_test_...",
#  PyCharm will try to debug the function by default. To prevent
#  this behaviour, add a single underscore to the start of test function names.
# ----------------------------------------------------------------------------
def _test_main():
    """
    Copy of the main function that prints information about the profitability
    of trading strategies

    YOU MAY MODIFY THIS FUNCTION IN ANY WAY YOU LIKE.
    """
    print_msg("Running _test_main...", as_header=True)
    # 1: Read CSV files
    stk_rets = read_stk_rets()
    ma_deals = read_ma_deals()
    org_ff = read_org_ff()

    # 2: Construct deal-level information
    ma_info = mk_ma_info(ma_deals)

    # 3: Compute abnormal returns 
    stk_arets = mk_stk_arets(
            stk_rets=stk_rets,
            org_ff=org_ff,
    )

    # 4: Expand events across event windows
    expanded_ma_info = expand_event_dates(
            events=ma_info,
            valid_dates=stk_rets.index,
    )

    # 5: Construct trading strategy returns

    # from buying the target and selling the acquirer
    buy_tgt_sell_acq_rets = mk_buy_tgt_sell_acq_rets(
            expanded_ma_info=expanded_ma_info,
            stk_rets=stk_rets,
            )
    print_msg("summarise_series(buy_tgt_sell_acq_rets) ->", 
              summarise_series(buy_tgt_sell_acq_rets), '')

    # from buying the target and selling the market
    buy_tgt_sell_mkt_rets = mk_buy_tgt_sell_mkt_rets(
            expanded_ma_info=expanded_ma_info,
            stk_arets=stk_arets,
            )
    print_msg("summarise_series(buy_tgt_sell_mkt_rets) ->", 
              summarise_series(buy_tgt_sell_mkt_rets), '')

    # 6: Build an event-time panel for target returns
    tgt_rets_by_event_time = mk_tgt_rets_by_event_time(
        stk_rets=stk_rets,
        expanded_ma_info=expanded_ma_info,
        )

    # 7: Compute event-time positive-return proportions
    prop_positive_tgt_rets = mk_prop_positive_tgt_rets(
        tgt_rets_by_event_time= tgt_rets_by_event_time)
    print_msg("summarise_series(prop_positive_tgt_rets- 0.5) ->", 
              summarise_series(prop_positive_tgt_rets - 0.5), '')


#====Ivan's addition===
def _test_mk_ma_info():
    """
    It prints:
      - The first few rows of the raw ma_deals table,
      - The first few rows of mk_ma_info(ma_deals),
      - A comparison of the number of deals vs the number of rows
        in mk_ma_info by announcement date.
    """
    print_msg("Running _test_mk_ma_info...", as_header=True)

    # Read real data
    ma_deals = read_ma_deals()
    ma_info = mk_ma_info(ma_deals)
    ma_deals['announcement'] = pd.to_datetime(ma_deals['announcement'])
    ma_info = mk_ma_info(ma_deals)

    # Show a snapshot of inputs and outputs
    print_msg("ma_deals.head() ->", ma_deals.head(), '')
    print_msg("mk_ma_info(ma_deals).head() ->", ma_info.head(), '')

    # For each announcement date:
    #   n_deals        = number of distinct dealno in the raw data
    #   n_rows_ma_info = number of rows produced by mk_ma_info
    deals_per_date = (
        ma_deals.groupby('announcement')['dealno']
                .nunique()
                .rename('n_deals')
    )

    rows_per_date = (
        ma_info.groupby('announcement')['dealno']
               .size()
               .rename('n_rows_ma_info')
    )

    check = (
        pd.concat([deals_per_date, rows_per_date], axis=1)
          .fillna(0)
          .astype(int)
    )

    print_msg(
        "Deals vs mk_ma_info rows by announcement (first 10 dates) ->",
        check.head(10),
        ''
    )

    # Highlight any dates where mk_ma_info has more rows than deals
    problem_dates = check[check['n_rows_ma_info'] > check['n_deals']]

    if not problem_dates.empty:
        print_msg(
            "Dates with more rows in mk_ma_info than deals ->",
            problem_dates,
            ''
        )

        # Show detailed comparison for the first problematic date
        example_date = problem_dates.index[0]

        print_msg(
            f"Raw ma_deals rows for announcement = {example_date} ->",
            ma_deals[ma_deals['announcement'] == example_date],
            ''
        )

        # ma_info.announcement is datetime64, so convert
        example_dt = pd.to_datetime(example_date)
        print_msg(
            f"mk_ma_info rows for announcement = {example_date} ->",
            ma_info[ma_info['announcement'] == example_dt],
            ''
        )
    else:
        print_msg(
            "No dates where mk_ma_info has more rows than deals "
            "(no duplication visible in this dataset).",
            '',
            ''
        )
def _test_mk_ma_info_has_both_tickers():
    """
    Check that mk_ma_info only includes deals that have BOTH
    an acquirer and a target in the original ma_deals table.
    """
    print_msg("Running _test_mk_ma_info_has_both_tickers...", as_header=True)

    # Read real data and build ma_info
    ma_deals = read_ma_deals()
    ma_info  = mk_ma_info(ma_deals)

    # Deals that have at least one acquirer row
    deals_with_acq = set(
        ma_deals.loc[ma_deals["firmtype"] == "acq", "dealno"]
    )

    # Deals that have at least one target row
    deals_with_tgt = set(
        ma_deals.loc[ma_deals["firmtype"] == "tgt", "dealno"]
    )

    # Deals that have BOTH acquirer and target in the raw data
    deals_with_both = deals_with_acq & deals_with_tgt

    # Deals that appear in mk_ma_info
    deals_in_ma_info = set(ma_info["dealno"])

    # Differences
    extra_in_ma_info = deals_in_ma_info - deals_with_both
    missing_from_ma_info = deals_with_both - deals_in_ma_info

    print_msg("Number of deals with an acquirer row ->",
              len(deals_with_acq), '')
    print_msg("Number of deals with a target row   ->",
              len(deals_with_tgt), '')
    print_msg("Number of deals with BOTH          ->",
              len(deals_with_both), '')
    print_msg("Number of deals in ma_info         ->",
              len(deals_in_ma_info), '')

    print_msg("Deals in ma_info but NOT in 'both' set ->",
              sorted(extra_in_ma_info), '')
    print_msg("Deals with both but missing from ma_info ->",
              sorted(missing_from_ma_info), '')

    # Sanity check: no missing tickers inside ma_info
    print_msg("Any missing acq in ma_info? ->",
              ma_info["acq"].isna().any(), '')
    print_msg("Any missing tgt in ma_info? ->",
              ma_info["tgt"].isna().any(), '')


# ----------------------------------------------------------------------------
#  Function to run all other tests
# ----------------------------------------------------------------------------
def run_tests():
    """
    Run all test functions.

    You may complete or extend this function in any way you like.
    """
    print("Running tests...\n")

    # Uncomment to run
    _test_main()

    # Add other function calls here
    #Ivan's addition, uncomment if needed.
    #_test_mk_ma_info() 
    #_test_mk_ma_info_has_both_tickers()

    
if __name__ == "__main__":
    run_tests()




