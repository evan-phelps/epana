import functools
import pickle
import requests
import datetime

import pandas as pd

from epana import throttle


def cached(func):
    func.cache = {}
    try:
        func.cache = pickle.load(open('%s.cache.pickle' % func.__name__, 'rb'))
    except FileNotFoundError:
        pass

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))
        try:
            return func.cache[key]
        except KeyError:
            func.cache[key] = result = func(*args, **kwargs)
            return result
    return wrapper


@throttle.throttle(per_sec=30)
@cached
def rxnorm_req(resource, **kwargs):
    is_json = True
    if 'rxnorm_base' not in kwargs:
        kwargs['rxnorm_base'] = 'https://rxnav.nlm.nih.gov/REST/'
    if 'json' in kwargs:
        is_json = kwargs['json']
    if is_json:
        resource += '.json'

    attrs = ['%s=%s' % (attr, val) for (attr, val) in
             kwargs.items() if attr != 'rxnorm_base']

    req = kwargs['rxnorm_base'] + resource + '?%s' % ('&'.join(attrs))

    try:
        resp = requests.get(req, timeout=(1, 1))
    except requests.exceptions.Timeout:
        try:
            resp = requests.get(req, timeout=(2, 2))
        except requests.exceptions.Timeout:
            try:
                resp = requests.get(req, timeout=(2, 5))
            except requests.exceptions.Timeout:
                print(kwargs)
                return None

    return resp.json() if is_json else resp


def coerce_rxcui(rxcui):
    json = rxnorm_req('rxcui/%s/status' % rxcui)
    status = json['rxcuiStatus']['status']

    if status in ('Retired', 'Unknown', 'Alien'):
        # warning('Cannot coerce! status = %s'%status)
        return None

    retval = json['rxcuiStatus']['minConceptGroup']['minConcept'][0]['rxcui']
    return retval


def get_status(rxcui):
    json = rxnorm_req('rxcui/%s/status' % rxcui)
    status = json['rxcuiStatus']['status']
    return status


tmp_n_get_TTY = 0


def get_TTY(rxcui):
    global tmp_n_get_TTY
    if rxcui is None or rxcui == '':
        return ''
    tmp_n_get_TTY += 1
    if tmp_n_get_TTY % 1000 == 0:
        print(datetime.datetime.now(), tmp_n_get_TTY, flush=True)
    json = rxnorm_req('rxcui/%s/property' % rxcui, propName='TTY')
    cgroup = json['propConceptGroup']
    if cgroup is None:
        return ''
    return cgroup['propConcept'][0]['propValue']


def get_props(rxcui, skip_coerce=False):
    json = rxnorm_req('rxcui/%s/properties' % rxcui)
    key = 'properties'
    props = None
    if json is None:
        if skip_coerce is False:
            rxcui_new = coerce_rxcui(rxcui)
            if rxcui_new is not None:
                props = get_props(coerce_rxcui(rxcui),
                                  skip_coerce=True)
    else:
        props = json[key]

    return props


def get_ins(rxcui):
    json = rxnorm_req('rxcui/%s/related' % rxcui, tty='IN')
    try:
        retval = [(y['rxcui'], y['name']) for y in
                  [x['conceptProperties'] for x in
                   json['relatedGroup']['conceptGroup']][0]]
    except KeyError as e:
        # warning('missing key %s'%e)
        return []
    return retval


def get_scd(rxcui):
    # https://rxnav.nlm.nih.gov/REST/rxcui/174742/related?tty=SBD+SBDF
    json = rxnorm_req('rxcui/%s/related' % rxcui, tty='SCD')
    try:
        retval = [(y['rxcui'], y['name']) for y in
                  [x['conceptProperties'] for x in
                   json['relatedGroup']['conceptGroup']][0]]
    except KeyError as e:
        # warning('missing key %s'%e)
        return []
    return retval


def get_rxcui_from_ndc(ndc):
    json = rxnorm_req('rxcui', idtype='NDC', id=ndc)
    try:
        return json['idGroup']['rxnormId'][0]
    except KeyError as e:
        # warning('missing key %s'%e)
        return None


def get_props_df(code):
    '''Wrapper of rxnorm get_props function adapted to work with
    DataFrame.apply. Argument "code" is expected to be the order or admin
    "med_code," which is an rxcui in CDW.  If the code exists, this function
    checks for RxNorm properties.
    '''
    props = None
    pnames = ['name', 'rxcui', 'synonym', 'tty']
    cnames = ['rxname', 'rxcui', 'rxsyn', 'rxtty']
    retval = None
    if code is not None and len(code) > 0:
        props = get_props(code)
    if props is None:
        retval = {cname: None for cname in cnames}
        retval['mo_code'] = code
    else:
        retval = {cname: props[pname]
                  for (cname, pname) in zip(cnames, pnames)}
        retval['mo_code'] = code
    return pd.Series(retval)


def get_rxcui(rxname):
    # https://rxnav.nlm.nih.gov/REST/rxcui?name=lipitor
    json = rxnorm_req('rxcui', name=rxname)
    try:
        retval = json['idGroup']['rxnormId'][0]
    except KeyError as e:
        # warning('missing key %s'%e)
        return []
    return retval


def get_related(rxcui):
    # https://rxnav.nlm.nih.gov/REST/rxcui/174742/related?tty=SBD+SBDF
    ttys = 'IN+PIN+MIN+SCDC+SCDF+SCDG+SCD+GPCK+BN+SBDC+SBDF+SBDG+SBD+BPCK+PSN'
    json = rxnorm_req('rxcui/%s/related' % rxcui,
                      tty=ttys)
    # pprint(json)
    try:
        cgs = json['relatedGroup']['conceptGroup']
        retval = [y for y in  # (y['rxcui'], y['name']) for y in
                  [(cps['rxcui'], cps['tty'], cps['name'])
                   for cg in cgs
                   if 'conceptProperties' in cg
                   for cps in cg['conceptProperties']
                   ]
                  ]
    except KeyError as e:
        print('missing key %s' % e)
        return []
    return retval


def get_rxcuis_related(rxcui):
    return [rxcui for (rxcui, _, _) in get_related(rxcui)]
