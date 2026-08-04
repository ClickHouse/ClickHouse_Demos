from arena.config import ModelCfg
from agents.llm import _parse_prices, apply_live_prices


def test_parse_prices_converts_per_token_to_per_million():
    data = {"data": [
        {"id": "openai/gpt-5.6-luna",
         "pricing": {"prompt": "0.00000015", "completion": "0.0000006"}},
        {"id": "no-pricing"},
    ]}
    out = _parse_prices(data)
    assert out["openai/gpt-5.6-luna"] == (0.15, 0.6)
    assert "no-pricing" not in out


def test_apply_live_prices_overrides_only_known_ids():
    models = [
        ModelCfg(id="openai/gpt-5.6-luna", name="mini", family="openai",
                 price_per_1m_in=9.0, price_per_1m_out=9.0),
        ModelCfg(id="unknown/model", name="unk", family="x",
                 price_per_1m_in=1.0, price_per_1m_out=2.0),
    ]
    prices = {"openai/gpt-5.6-luna": (0.15, 0.6)}
    out = apply_live_prices(models, prices)
    assert (out[0].price_per_1m_in, out[0].price_per_1m_out) == (0.15, 0.6)
    assert (out[1].price_per_1m_in, out[1].price_per_1m_out) == (1.0, 2.0)
