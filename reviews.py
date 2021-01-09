from selectorlib import Extractor
import requests
import csv
from dateutil import parser as dateparser

# Create an Extractor by reading from the YAML file
e = Extractor.from_yaml_file('selectors.yml')
PAGE_NUMBER = "&pageNumber="


def scrape(base_url, limit):
    headers = {
        'authority': 'www.amazon.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }

    writer = csv.DictWriter(outfile,
                            fieldnames=["title", "content", "date", "variant", "images", "verified", "author", "rating",
                                        "product", "url"], quoting=csv.QUOTE_ALL)
    writer.writeheader()

    page_index = 1
    review_count = 0
    review_data = []
    while review_count < limit:
        next_url = base_url + PAGE_NUMBER + str(page_index)
        page_index += 1

        data_from_extractor = download(next_url,headers)

        data_set = load_n_transform(data_from_extractor)

        review_data.extend(data_set)
        review_count = len(review_data)

        writer.writerows(data_set)
    return 1



def load_n_transform(data):
    data_set = []
    if data:
        for r in data["reviews"]:
            r["product"] = data["product_title"]
            r['url'] = url
            if 'verified' in r:
                if 'Verified Purchase' in r['verified']:
                    r['verified'] = 'Yes'
                else:
                    r['verified'] = 'Yes'
            r['rating'] = r['rating'].split(' out of')[0]
            date_posted = r['date'].split('on ')[-1]
            if r['images']:
                r['images'] = "\n".join(r['images'])
            r['date'] = dateparser.parse(date_posted).strftime('%d %b %Y')
            data_set.append(r)
        return data_set

def download(url, headers):
    # Download the page using requests
    print("Downloading %s" %url)
    r = requests.get(url, headers=headers)
    # Simple check to check if page was blocked (Usually 503)
    if r.status_code > 500:
        if "To discuss automated access to Amazon data please contact" in r.text:
            print("Page %s was blocked by Amazon. Please try using better proxies\n" % url)
        else:
            print("Page %s must have been blocked by Amazon as the status code was %d" % (url, r.status_code))
    # Pass the HTML of the page and create

    return e.extract(r.text)


def print_data(data):
    for row in data:
        print(row)

# product_data = []
with open("urls.txt", 'r') as url_list, open('data.csv', 'w') as outfile:
    for url in url_list.readlines():
        data = scrape(url, limit=20000)

