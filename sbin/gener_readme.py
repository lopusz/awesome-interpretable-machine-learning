#!/usr/bin/env python3

import argparse
import json
import re
import os
import sys
import time 


import urllib.request
import xml.etree.ElementTree as ET

KEY_FIELD_NAME = '0KEY_'

def parse_argv(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--readme-template', dest='readme_template',
                        action='store', required=True)
    parser.add_argument('--cache-fname', dest='cache_fname',
                        action='store', required=False, default=None)
    return parser.parse_args(argv)


def load_cache(cache_fname):
    cache = {}
    if os.path.isfile(cache_fname):
        with open(cache_fname, 'rt') as f:
            for line in f:
                r = json.loads(line)
                key = r[KEY_FIELD_NAME]
                cache[key] = r
    return cache


def save_cache(cache_fname, cache):
    
    with open(cache_fname, 'wt') as f:
        for k in sorted(cache.keys()):
            f.write(json.dumps(cache[k], sort_keys=True)+'\n')


def extract_reference(line):
    ENUMERATION_CHARS = { '+' }
    BEG_REFERENCE_CHAR = '<'
    END_REFERENCE_CHAR = '>'

    def _get_line_prefix(line, enumerator_char):
        return line.split(sep=enumerator_char, maxsplit=1)[0]

    res = None
    line_stripped = line.strip()
    if line_stripped and line_stripped[0] in ENUMERATION_CHARS:
        line_reference = line_stripped[1:].strip()
        if (line_reference[0] == BEG_REFERENCE_CHAR and line_reference[-1] == END_REFERENCE_CHAR):
            enumerator_char = line_stripped[0]
            res = { 'source_id' : line_reference[1],
                    'id' : line_reference[3:-1],
                    'enumerator_char' : enumerator_char,
                    'line_prefix' : _get_line_prefix(line, enumerator_char) }
    return res


def _get_cache_key(ref):
    return ref['source_id']+':'+ref['id']
 

def parse_generic_xml(root):

    ATTRIB_KEY = '_attrib'
    
    def _normalize_tag(s):
        i = s.rfind('}')
        return s[(i+1):]

    def _insert_and_listify(res, key, val):

        if key in res:
            val_cur = res[key]
            if isinstance(val_cur, list):
                res[key].append(val)
            else:
                res[key] = [val_cur] + [val]
        else:
            res[key] = val
        return res

    res = {}
    
    for child in root:
        if len(child) > 0:
            key = _normalize_tag(child.tag)
            val = parse_generic_xml(child)
            if len(child.attrib) > 0:
                val[ATTRIB_KEY] = child.attrib
            res = _insert_and_listify(res, key, val)
        else:
            key = _normalize_tag(child.tag)
            val = child.text
            if len(child.attrib) > 0:
                val_new = { 'val': val, ATTRIB_KEY: child.attrib }
                val = val_new
            res = _insert_and_listify(res, key, val)
    return res


def fetch_raw_metadata_for_reference_arxiv(ref):
    OAI_PMH_URL = 'http://export.arxiv.org/oai2'
    QUERY_FORMAT = '?verb=GetRecord&identifier=oai:arXiv.org:%s&metadataPrefix=arXiv'
 
    query = QUERY_FORMAT % ref['id']
    finished = False
    while not finished:
        try:
            url = OAI_PMH_URL + query
            #print('Fetching %s' % url, file=sys.stderr)
            result = urllib.request.urlopen(url).read()
            time.sleep(5)
            finished = True
        except urllib.error.HTTPError as e:
            if e.code == 503:
                retry_after = int(e.headers['Retry-After'])
                print('Sleeping %d...' % retry_after, file=sys.stderr)
                time.sleep(retry_after)
            else:
                raise e
    return parse_generic_xml(ET.fromstring(result))  

  
def fetch_raw_metadata_for_reference_doi(ref):
    CROSSREF_API_URL = 'http://api.crossref.org/works/'
 
    url = CROSSREF_API_URL + ref['id'] 
    #print('Fetching %s' % url, file=sys.stderr)
    result = urllib.request.urlopen(url).read()
    time.sleep(5)
    finished = True
    return json.loads(result.decode('utf-8'))


def clean_raw_metadata_for_reference_arxiv(met_raw):
    
    def _normalize_title(title):
        title_norm = title.replace('\n', ' ')
        title_norm = re.sub(r' +', ' ', title_norm)
        return title_norm
   

    def _normalize_authors(authors):
        return [ [ a['keyname'], a['forenames'] ] for a in authors ]


    def _normalize_arxiv_id(arxiv_id):
        return arxiv_id.replace('oai:arXiv.org:', '')


    met = {}
    title_raw = met_raw['GetRecord']['record']['metadata']['arXiv']['title']
    authors_raw = met_raw['GetRecord']['record']['metadata']['arXiv']['authors']['author']
    
    met['title'] = _normalize_title(title_raw) 
    met['authors'] = _normalize_authors(authors_raw)
    arxiv_id_raw = met_raw['GetRecord']['record']['header']['identifier'] 
    met['arxiv_id'] = _normalize_arxiv_id(arxiv_id_raw) 

    try:
        doi = met_raw['GetRecord']['record']['metadata']['arXiv']['doi']
        met['doi'] = doi 
    except Exception as e:
        met['doi'] = None
    return met
    

def clean_raw_metadata_for_reference_doi(met_raw):
    title_parts = met_raw['message']['title']
    
    met = {}

    title=''
    for t in title_parts:
        title += ' ' + t
    
    authors  =[ [ a['family'], a['given'] ]  for a in met_raw['message']['author'] ]

    met['title'] = title[1:]
    met['authors'] = authors
    met['doi'] =  met_raw['message']['DOI']
    return met 


def fetch_metadata_for_reference(ref, cache):
    
    if ref['source_id'] == 'a':
        fetch_raw = fetch_raw_metadata_for_reference_arxiv
        clean_raw = clean_raw_metadata_for_reference_arxiv
    else:
        fetch_raw = fetch_raw_metadata_for_reference_doi
        clean_raw = clean_raw_metadata_for_reference_doi
    
    cache_key = _get_cache_key(ref)
    met_raw = cache.get(cache_key, None)

    if met_raw is None:
        met_raw = fetch_raw(ref)
        met_raw[KEY_FIELD_NAME] = cache_key
        cache[cache_key] = met_raw
    met = clean_raw(met_raw)
    return met


def convert_reference_and_metadata_to_lines(ref, met):
    content = ref['line_prefix'] + ref['enumerator_char'] + ' ' + met['title']
    authors = ''
   
    for a in met['authors']:
        authors += ', ' + a[1] + ' ' + a[0]
    content += ' by ' + authors[2:] + '\n'
    prefix =  ref['line_prefix'] + '  '  + ref['enumerator_char'] + ' '
    arxiv_id = met.get('arxiv_id', None)
    if arxiv_id is not None:
        content += prefix + 'https://arxiv.org/pdf/' + arxiv_id + '\n'
    doi = met.get('doi', None)
    if doi is not None:
        content += prefix + 'https://dx.doi.org/' + doi + '\n'
    return content


def main(args):

    cache = {}        
    if args.cache_fname is not None:
        cache = load_cache(args.cache_fname)
    n0_cache = len(cache) 

    with open(args.readme_template, 'r') as f:
        for line in f:
            ref = extract_reference(line)

            if ref is not None:
                met = fetch_metadata_for_reference(ref, cache)
                lines = convert_reference_and_metadata_to_lines(ref, met)
                print(lines, end='')
            else:
                print(line, end='')
        print(len(cache))        
        if args.cache_fname is not None and n0_cache != len(cache):
            save_cache(args.cache_fname, cache)


if __name__ == '__main__':
    args = parse_argv(sys.argv[1:])
    main(args)
