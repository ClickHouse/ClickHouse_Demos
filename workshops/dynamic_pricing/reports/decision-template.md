# Mock pricing decision

Export a reviewed decision with `workshop export-report --participant <owner> --decision <id>`. The command writes persisted facts to `reports/exports` as JSON and Markdown.

Review these facts:

- Source versions, ages, and snapshot cutoff
- Current price, proposed current-window price, and keep-price baseline
- Projected receipts, gross margin, selling costs, disposal costs, and net recovery
- Demand variability and parameter sensitivity
- Policy reasons and learner review identity
- Application status and acknowledged receipt

The second-window plan remains projected evidence. A proposal does not change the applied mock price. Use the receipt and subsequent sale to verify application.
