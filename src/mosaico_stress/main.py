import typer

from mosaico_stress.commands import download, upload

app = typer.Typer(
    help="Mosaico Stress Test Extension — upload/download throughput benchmarks.",
    no_args_is_help=True,
)

app.add_typer(upload.app, name="upload", help="Stress test: concurrent data upload.")
app.add_typer(download.app, name="download", help="Stress test: concurrent data download.")


if __name__ == "__main__":
    app()
