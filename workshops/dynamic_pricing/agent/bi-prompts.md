# Retail Pricing Strategist prompts

Create **Retail Pricing Strategist** in [ClickHouse Agents](https://ai.clickhouse.cloud/).
Choose an available model in its UI, paste `retail-pricing-strategist.md` as instructions,
and attach `retail-data-dictionary.md` and `retail-pricing-policy.md` as context. Enable
the native ClickHouse tool for your workshop service. No model API key is needed in the
local workshop environment. The [managed product overview](https://clickhouse.com/blog/clickhouse-agents-beta)
describes the agent builder, context files and governed tools.

1. “Identify the current run, cutoff, delivery age and model. In district 1, which five store/product pairs have the most stock at risk of expiry? Show the SQL and explain eligibility.”
2. “For the highest eligible opportunity, compare every permitted price, including unchanged price. Recommend the highest modeled profit and report receipts, units, waste and the uncertainty assumptions. Give me the exact store, SKU and price to review.”
3. “I approved the price in the workbench. Here is the receipt UUID: PASTE_RECEIPT_UUID. Query its observed results, compare the applied and unchanged-price branches, and explain what the synthetic experiment does and does not establish.”

The agent recommends from ClickHouse evidence. Review and apply in the workbench, then
return the receipt UUID to the conversation. The agent's instructions do not grant
write access or replace the learner's approval.
