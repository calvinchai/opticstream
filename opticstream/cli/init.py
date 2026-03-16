from cyclopts import App
from .root import app

init_cli = app.command(App(name="init"))


@init_cli.command
def blocks():
    from ..config.psoct_scan_config import PSOCTScanConfig
    from ..config.lsm_scan_config import LSMScanConfig

    PSOCTScanConfig.register_type_and_schema()
    LSMScanConfig.register_type_and_schema()

    print("Blocks registered successfully")