"""Shared calendar and synthetic catalog for the chain-wide workshop."""
from datetime import UTC, datetime, timedelta

START = datetime(2026, 1, 2, 2, tzinfo=UTC)
CUTOVER = datetime(2026, 1, 30, 2, tzinfo=UTC)
DAYS = 28
OPEN_UTC_HOUR = 2
TRADING_HOURS = 8


def retail_catalog() -> tuple[list[dict], list[dict]]:
    stores = [{'store_id': i, 'store_name': f'Store {i}',
               'district_id': (i - 1) // 250 + 1,
               'district_name': f'District {(i - 1) // 250 + 1}',
               'store_group': (i - 1) % 4, 'timezone': 'Asia/Singapore'}
              for i in range(1, 5001)]
    names = ['Chicken sandwich', 'Egg sandwich', 'Tuna sandwich', 'Vegetable wrap',
             'Chicken wrap', 'Rice bowl', 'Noodle bowl', 'Pasta salad', 'Fruit cup',
             'Yogurt cup', 'Sushi box', 'Salmon onigiri', 'Tuna onigiri', 'Egg roll',
             'Chicken salad', 'Vegetable salad', 'Breakfast bun', 'Curry puff',
             'Cheese toast', 'Ready-to-eat soup']
    products = []
    for n, name in enumerate(names):
        price = 800 + (n % 5) * 100
        products.append({'sku': 101 + n, 'product_name': name, 'category': 'Ready-to-eat food',
                         'reference_price_minor': price, 'unit_cost_minor': price // 2,
                         'selling_cost_minor': 20, 'disposal_cost_minor': 50,
                         'permitted_prices_minor': list(range(price * 3 // 4 // 50 * 50, price + 1, 50)),
                         'currency': 'SGD'})
    return stores, products


def trading_slots():
    for day in range(DAYS):
        for hour in range(TRADING_HOURS):
            yield START + timedelta(days=day, hours=hour)


def calendar_factor(at: datetime) -> float:
    return (0.8 + 0.06 * (at.hour - OPEN_UTC_HOUR)) * (1.15 if at.weekday() >= 5 else 1.0)


def reference_rate(sku: int, store_group: int) -> float:
    return (4 + .15 * (sku - 101)) * (0.8 + .15 * store_group)
