from .binance_utils import get_exchange_info


def test_exchange_info_is_dict():
    info = get_exchange_info()
    assert isinstance(info, dict)
