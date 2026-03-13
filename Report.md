# Midterm Report

**Student Name:** Panat Kaewlangka
**Student ID:** 650610xxx
**Date:** March 13, 2026

---

## Part 1: Data Exploration Answers

> How many total transactions are there?

There are **72,586,000** total transactions in the dataset.

> How many unique family members, merchants, and categories?

- **Unique Family Members:** 4 members
- **Unique Merchants:** 50 merchants
- **Unique Categories:** 18 categories

> How many rows have null or empty `amount` values?

There are **1,450,421** rows with null or empty amount values.

> What is the date range of the transactions?

The raw transactions range from **2010-01-01** to **2025-12-31**.

---

## Part 4: Join Analysis

> What join type did you use for enriching transactions, and why?

I used a **Left Join** because we need to preserve all transaction records even if they don't have a matching merchant or category in the lookup tables. Using an inner join would result in data loss for any transaction with an "orphan" ID.

> How many transactions have no matching merchant in the merchants table?

There are **212,089** transactions that have no matching merchant in the merchants table.

> What would happen if you used an inner join instead?

If an inner join was used, those **212,089** transactions would be completely removed from the final dataset, leading to an underestimation of total family spending and incomplete financial reporting.

---

## Part 5: Analytics Insights

> Look at the average transaction amount per year table. Do you notice a trend? Calculate the approximate year-over-year percentage change. What might explain this?

Yes, there is a clear upward trend in the average transaction amount. It increases steadily every year.

| Year | Avg Amount | YoY Change (%) |
|------|-----------|----------------|
| 2016 | 55.98 | - |
| 2017 | 57.11 | +2.02% |
| 2018 | 58.33 | +2.14% |
| 2019 | 59.46 | +1.94% |
| 2020 | 60.58 | +1.88% |
| 2021 | 61.87 | +2.13% |
| 2022 | 63.08 | +1.95% |
| 2023 | 64.41 | +2.11% |
| 2024 | 65.54 | +1.75% |
| 2025 | 66.94 | +2.13% |

**Calculation Example (2017):** `((57.11 - 55.98) / 55.98) * 100 = 2.02%`

_Your explanation:_ The average growth rate is approximately **2% per year**. This steady increase is likely explained by **inflation**, as the cost of goods and services typically rises over time. It could also reflect a lifestyle change where the family gradually buys higher-quality items.

> Which category has the highest total spending? Which has grown the fastest over 10 years?

**Groceries** has the highest total spending by a significant margin. Based on the year-over-year increase in average transaction costs, **Electronics** and **Education** are among the fastest-growing categories in terms of total volume.

> Compare spending between family members. Who spends the most? On what?

**MEM01** spends the most (Total: 1.95B). Based on typical household patterns, this is likely the primary breadwinner or the person responsible for major household purchases like Groceries and Utilities.

> What percentage of transactions fall in each spending tier? Has this distribution changed over the years?

- **Small (10-50):** ~48% (Largest group)
- **Medium (50-200):** ~30%
- **Micro (<10):** ~14%
- **Large (>=200):** ~5%

The distribution remains relatively stable, but there is a slight shift from 'small' to 'medium' as average prices rise due to inflation.

---

## Section A: Data Architecture Questions

### Q1. Merchant Name Change
To update a merchant's name, we only need to update the record in the `merchants.csv` lookup table. Since the pipeline uses a Join at the **Analytics layer**, we only need to reprocess the **Analytics layer** (specifically the `enriched_transactions` and summary tables). The Raw and Staged layers remain unchanged because they store the original IDs.

### Q2. New Family Member
A new family member can be added simply by having their `member_id` appear in new transactions in `transactions.csv`. Since our pipeline is dynamic and uses `groupBy("member_id")`, the system will automatically include the new member in the `yearly_by_member` report during the next run without any code changes.

### Q3. Average Monthly Grocery Spending
The process involves: 1. Filtering **Staged** transactions for `category_id` linked to "Groceries". 2. **Joining** with a Date lookup to extract Month and Year. 3. **Aggregating** using `groupBy("year", "month")` and `avg("amount")`. 4. Finally, calculating the mean of those monthly averages.

### Q4. Duplicate Transactions
Currently, the pipeline treats every row as a unique transaction. To handle duplicates, I would add a `df.dropDuplicates(["transaction_id"])` step in the **Staged layer**. This ensures that even if a transaction is exported twice, it only counts once toward the total spending.

### Q5. Data Backup & Recovery
My strategy uses the **Medallion Architecture**. By keeping the **Raw layer** (immutable Parquet files), we can always reconstruct the entire pipeline if the Staged or Analytics layers are corrupted. We could lose at most one month of data (RPO) if the crash happens right before the monthly export.

---

## Section B: Engineering Questions

### Q6. CI/CD Pipeline
A CI/CD pipeline (e.g., GitHub Actions) would trigger on every **Pull Request** or **Push** to the main branch. It would: 1. Set up a Python environment. 2. Install dependencies. 3. Run **Pytest** (Unit Tests) for all transformations. 4. Run a Linter (like Flake8) to ensure code quality.

### Q7. Monthly Automation with Orchestration
I would use an orchestrator like **Apache Airflow**. The DAG would look like this:

```
Your DAG:
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Fetch CSV   │ ──▶ │  Load to Raw │ ──▶ │ Clean Staged │ ──▶ │ Generate BI  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

---

## Section C: Analytics Insights

### Q8. Price Trend Analysis
The exact rate of increase is approximately **2.03% per year**. This is consistent with national inflation targets. It is mostly consistent across categories, though "Wants" (like Electronics) tend to fluctuate more than "Needs" (like Groceries).

### Q9. Spending Recommendations
1. **Optimize Groceries:** Since it's the highest category, using bulk buying or loyalty programs could save a significant amount.
2. **Review "Wants":** Spending on 'wants' is nearly 44%; reducing this by 5% would significantly increase yearly savings.
3. **Limit 'Large' Purchases:** Large transactions represent only 5% of volume but a huge portion of value; implementing an approval rule for items > $200 could help.

### Q10. Needs vs Wants

| Budget Type | Total Spending | Percentage |
|-------------|---------------|------------|
| Needs | 2.16B | 49.8% |
| Wants | 1.90B | 43.8% |
| Savings | 0.28B | 6.4% |
