import requests
import pandas as pd

def get_odata(endpoint, query_option, api_key, proxies = {}):

    # setup variables
    headers = {'Ocp-Apim-Subscription-Key': api_key}
    proxies = proxies
    url = "https://api.stats.govt.nz/opendata/v1/" + endpoint + '/Observations?' + query_option
    top_query = "$top" in query_option
    results = pd.DataFrame()
    #print(endpoint, query_option, end=': ')
    # continue getting results while there are more pages
    while url:

        try:
            r = requests.get(url,headers=headers,proxies=proxies)
            r.raise_for_status()

        # raise request errors
        except requests.HTTPError as exception:
            print(exception)
            print(r.text)
            break

        df = pd.json_normalize(r.json()['value'])
        results = pd.concat([results,df])

        # get the next page url
        try:
            url = r.json()['@odata.nextLink']
            # return just the first page if $top was used
            if top_query:
                url = None
        except KeyError:
            url = None
        # show progress
        #print('.', end = ' ', flush = True)

    #print(len(results.index),'Obs retrieved')
    return results