from warcio.archiveiterator import ArchiveIterator
import requests
import pandas as pd

def get_url_and_resp(record):
    if record.rec_type == 'warcinfo':
        return('warcinfo', record.raw_stream.read()[:20])

    elif record.rec_type == 'response':
        if record.http_headers is not None:
            if record.http_headers['Content-Type'] is not None:
                if 'text/html' in record.http_headers['Content-Type']:
                    return ('target uri', record.rec_headers.get_header('WARC-Target-URI'), record.content_stream().read()[:20])
                else:
                    return ('other content type', record.http_headers['Content-Type'])
            else:
                return ('no content type header', record.http_headers)
        else:
            return ('no headers',vars(record))
    elif record.rec_type not in ('warcinfo', 'response'):
        return ('other record type', record.rec_type)

def get_urls_and_resps(resp):
    '''
    Given the requests response of a url for a downloadable WARC file, gets the urls and the beginning of the content stream if available
    Returns a list of tuples with two strings each
    '''
    results = []

    for record in ArchiveIterator(resp.raw, arc2warc=True):
        results.append(get_url_and_resp(record))

    return results