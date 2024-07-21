import importlib

import azure.functions as func

from azfn_starter_kit.azfn import shared_bp
from azfn_starter_kit.utilities.logger import get_logger

_LOGGER = get_logger(__name__)
BASE_MODULE = "azfn_starter_kit.azfn"

USECASES_MODULES = ["basic_data_flow_example"]
BLUEPRINT_MODULES = ["activities", "orchestrators", "triggers"]

# Import blueprints
for directory in USECASES_MODULES:
    for module in BLUEPRINT_MODULES:
        importlib.import_module(f"{BASE_MODULE}.{directory}.{module}.blueprint")

app = func.FunctionApp()
app.register_functions(shared_bp)
