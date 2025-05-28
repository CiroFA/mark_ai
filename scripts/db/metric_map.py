"""
metric_map.py

Maps each supported metric to a tuple (table_name, column_name) matching the
MySQL schema used by the handlers in scripts/db/*.  Keep this file in sync
with both `_SUPPORTED_METRICS` in parse_num_query.py and the actual database
schema.

Feel free to extend the dictionary as you add new metrics or tables.
"""

METRIC_MAP: dict[str, tuple[str, str]] = {
    # ---- balance_sheet core metrics -----------------------------------------
    "total_assets":              ("balance_sheet", "Total_Assets"),
    "stockholders_equity":       ("balance_sheet", "Stockholders_Equity"),
    "total_debt":                ("balance_sheet", "Total_Debt"),
    "net_debt":                  ("balance_sheet", "Net_Debt"),
    "long_term_debt":            ("balance_sheet", "Long_Term_Debt"),
    "working_capital":           ("balance_sheet", "Working_Capital"),
    "cash_and_cash_equivalents": ("balance_sheet", "Cash_And_Cash_Equivalents"),
    "tangible_book_value":       ("balance_sheet", "Tangible_Book_Value"),
    "net_p_p_e":                 ("balance_sheet", "net_p_p_e"),
    "inventory":                 ("balance_sheet", "Inventory"),
    "accounts_receivable":       ("balance_sheet", "Accounts_Receivable"),

    # ---- cashflow core metrics --------------------------------------------
    "free_cash_flow":              ("cashflow", "Free_Cash_Flow"),
    "operating_cash_flow":         ("cashflow", "Operating_Cash_Flow"),
    "investing_cash_flow":         ("cashflow", "Investing_Cash_Flow"),
    "financing_cash_flow":         ("cashflow", "Financing_Cash_Flow"),
    "capital_expenditure":         ("cashflow", "Capital_Expenditure"),
    "interest_paid":               ("cashflow", "Interest_Paid_Supplemental_Data"),
    "dividends_paid":              ("cashflow", "Cash_Dividends_Paid"),

    # ---- dividends core metrics --------------------------------------------
    "dividend":                    ("dividends", "dividend"),

    # ---- financials core metrics ------------------------------------------
    "total_revenue":                     ("financials", "Total_Revenue"),
    "operating_revenue":                 ("financials", "Operating_Revenue"),
    "gross_profit":                      ("financials", "Gross_Profit"),
    "cost_of_revenue":                   ("financials", "Cost_Of_Revenue"),
    "ebitda":                            ("financials", "EBITDA"),
    "ebit":                              ("financials", "EBIT"),
    "operating_income":                  ("financials", "Operating_Income"),
    "total_expenses":                    ("financials", "Total_Expenses"),
    "net_income":                        ("financials", "Net_Income"),
    "diluted_eps":                       ("financials", "Diluted_EPS"),
    "basic_eps":                         ("financials", "Basic_EPS"),
    "normalized_ebitda":                 ("financials", "Normalized_EBITDA"),
    "pretax_income":                     ("financials", "Pretax_Income"),
    "tax_provision":                     ("financials", "Tax_Provision"),
    "research_and_development":          ("financials", "Research_And_Development"),
    "selling_general_and_administration":("financials", "Selling_General_And_Administration"),
    "interest_expense":                  ("financials", "Interest_Expense"),
    "depreciation_and_amortization":     ("financials", "Depreciation_And_Amortization_In_Income_Statement"),

    # ---- history core metrics ---------------------------------------------
    "open_price":                ("history", "open"),
    "high_price":                ("history", "high"),
    "low_price":                 ("history", "low"),
    "close_price":               ("history", "close"),
    "daily_volume":              ("history", "volume"),
    "daily_dividends":           ("history", "dividends"),
    "stock_splits_history":      ("history", "stock_splits"),

    # ---- info core metrics ----------------------------------------------
    "full_time_employees":         ("info", "fullTimeEmployees"),
    "payout_ratio":                ("info", "payoutRatio"),
    "beta":                        ("info", "beta"),
    "trailing_pe":                 ("info", "trailingPE"),
    "forward_pe":                  ("info", "forwardPE"),
    "enterprise_value":            ("info", "enterpriseValue"),
    "book_value":                  ("info", "bookValue"),
    "price_to_book":               ("info", "priceToBook"),
    "profit_margins":              ("info", "profitMargins"),
    "float_shares":                ("info", "floatShares"),
    "shares_outstanding":          ("info", "sharesOutstanding"),
    "short_ratio":                 ("info", "shortRatio"),
    "average_analyst_rating":      ("info", "averageAnalystRating"),

    # ---- recommendations core metrics ------------------------------------
    "strong_buy":            ("recommendations", "strongBuy"),
    "buy":                   ("recommendations", "buy"),
    "hold":                  ("recommendations", "hold"),
    "sell":                  ("recommendations", "sell"),
    "strong_sell":           ("recommendations", "strongSell"),

    # ---- splits core metrics ----------------------------------------------
    "split_ratio":                   ("splits", "split_ratio"),

    # ---- sustainability core metrics --------------------------------------
    "total_esg":                  ("sustainability", "totalEsg"),
    "environment_score":          ("sustainability", "environmentScore"),
    "social_score":               ("sustainability", "socialScore"),
    "governance_score":           ("sustainability", "governanceScore"),
    "highest_controversy":        ("sustainability", "highestControversy"),
    "esg_performance":            ("sustainability", "esgPerformance"),
    "peer_count":                 ("sustainability", "peerCount"),
    "overall_percentile":         ("sustainability", "percentile"),
    "environment_percentile":     ("sustainability", "environmentPercentile"),
    "social_percentile":          ("sustainability", "socialPercentile"),
    "governance_percentile":      ("sustainability", "governancePercentile"),
}

# ---- alias per metriche comuni (input in linguaggio naturale) ------------
ALIAS_MAP: dict[str, str] = {
    "earnings": "net_income",
    "net earnings": "net_income",
    "revenue": "total_revenue",
    "sales": "operating_revenue",
    "employees": "full_time_employees",
    "p/e ratio": "trailing_pe",
    "pe ratio": "trailing_pe",
    "esg score": "total_esg",
    "ev": "enterprise_value",
    "book-to-price": "price_to_book",
    "eps": "diluted_eps",
}

# ---- mappa tabella->metriche ---------------------------------------------
from collections import defaultdict

TABLE_METRIC_MAP = defaultdict(list)
for key, (table, _) in METRIC_MAP.items():
    TABLE_METRIC_MAP[table].append(key)

