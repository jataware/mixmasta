"""Console script for mixmasta."""
import sys
import click
from .download import download_and_clean


@click.command()
@click.argument('command')
def main(command):
    """Console script for mixmasta."""
    if command == 'download':
    	download_and_clean()
    else:
        click.echo("Replace this message by putting your code into "
                   "mixmasta.cli.main")
        click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
