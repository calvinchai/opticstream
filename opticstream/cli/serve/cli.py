from cyclopts import App
from opticstream.cli.root import app

serve_cli = app.command(App(name="serve"))
