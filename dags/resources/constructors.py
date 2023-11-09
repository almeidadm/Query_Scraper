import locale
from datetime import datetime, timedelta
from pathlib import Path


def make_path(
    root: str = './',
    date: datetime.date = datetime.today(),
    add: list = ['prints'],
) -> str:
    """
    Args:
        root [str]: string para o diretório inicial
        date [datetime]: tipo datetime para a data a ser utilizada
        add [list]: pastas adicionais

    Returns:
        location [str]: Uma string com o caminho parcial criado a partir da data de referencia
    """
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    location = root + date.strftime(
        f'/%Y/%B/%Y_%m_%d'
    )

    Path(location).mkdir(parents=True, exist_ok=True)
    for fold in add:
        Path(f'{location}/{fold}').mkdir(parents=True, exist_ok=True)
    print(f'Create {location} folder')
    return location

