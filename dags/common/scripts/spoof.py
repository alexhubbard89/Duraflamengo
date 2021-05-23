import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
# from fake_useragent import UserAgent
from io import StringIO

class Ip_Spoofer(object):
    """
    This will be used to find a random IP and Port to proxy to.
    The need for this is because congress Blacklists a website when
    it looks like a bot, and doesn't allow blacklisted IPs to access
    their website. So to get aroudn this I am using a proxy IP address.
    """

    def request_page(self, url, collection_type=None, return_type='bs', print_url=False):
        """
        Because there are problems requesting pages measure have to be
        put in place to get around the limitations. Rather than writing
        redundant code this function will be used to handle url requests.
        """

        counter = 0
        status_good = False
        # ua = UserAgent(use_cache_server=False, verify_ssl=False)

        if print_url == True:
            print(url)

        while status_good == False:
            try:
                ## Get prxoy IP address
                ip = str(self.ip_df.loc[0, 'ip'])
                port = str(self.ip_df.loc[0, 'port'])
                s = requests.session()
                proxies = {
                  'http': '{}:{}'.format(ip, port),
                }
                s.proxies.update(proxies)
                a = requests.adapters.HTTPAdapter(max_retries=5)
                if 'https://' in url:
                    s.mount('https://', a)
                else:
                    s.mount('http://', a)
                # s.headers.update({'User-Agent': ua.random})
                r = s.get(url)

                if print_url == True:
                    print(r.status_code)
                if r.status_code == 200:
                    if "400 Bad Request" not in str(r.content):
                        status_good = True
                        if return_type == 'json':
                            return r.json()
                        elif return_type == 'csv':
                            data = r.content.decode('utf8')
                            return pd.read_csv(StringIO(data))
                        elif return_type == 'bs':
                            return BeautifulSoup(r.content, features='html.parser')
                elif r.status_code == 404:
                    return 'Status: 404'

                counter +=1
                self.ip_df = Ip_Spoofer.random_ip()
                if counter > 10:
                    break
            except:
                self.ip_df = Ip_Spoofer.random_ip()

    @staticmethod
    def random_ip():
        """
        User service to find random IP and port
        Args:
            None
        Returns:
            Dataframe with ip address and port
        """
        count = 1
        while count < 10:
            try:
                r = requests.get('https://api.getproxylist.com/proxy?apiKey={}&country[]=US'.format(os.environ['PROXY_LIST']))
                ip_df = pd.DataFrame([[r.json()['ip'], r.json()['port']]], columns=['ip', 'port']).reset_index(drop=True)
                return ip_df
            except:
                count += 1

    def __init__(self):
        self.ip_df = Ip_Spoofer.random_ip()