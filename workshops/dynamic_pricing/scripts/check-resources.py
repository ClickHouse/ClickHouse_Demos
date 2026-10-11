"""Validate the local secret-free inventory without contacting Cloud."""
from pricing_lab.settings import Settings
from infra.provision import ResourceInventory,check_inventory

settings=Settings.from_env()
inventory=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text())
check_inventory(inventory,settings)
print('Resource inventory matches the configured learner and organization. Cloud ownership remains a separate check.')
