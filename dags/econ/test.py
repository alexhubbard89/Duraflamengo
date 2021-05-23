import requests
from common.scripts.spoof import Ip_Spoofer

def test_function():

    url = 'https://www.marketwatch.com/investing/stock/MRNA/analystestimates'
    spoofer_obj = Ip_Spoofer()
    page = spoofer_obj.request_page(url, print_url=False)

    return True
