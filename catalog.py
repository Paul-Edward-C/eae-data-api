"""
Series catalogue
================
Turns the 1.4M rows of the `series` table into a browsable guide.

Names are comma-separated paths ("China, CPI, Food, % YoY, 3mma"), so most of the
apparent size is the same concept repeated across transforms and units. The
catalogue folds that away in three steps:

    variant   trailing transform segments (% YoY, 3mma, SA, z score ...) plus one unit
              segment (USD bn, % of GDP, Index ...) — shown as chips, not rows
    concept   what is left: the thing a user is actually looking for
    tree      economy -> topic (source parquet, frequency suffix dropped)
              -> group -> subgroup, where group/subgroup are the next two segments
              after the prefix every name in the topic shares

Cross-country files (CEPII, TiVA, OECD, the trade files' partner positions) put an
economy name part-way down the path. That position is pulled out as a facet, so the
tree shows "Exports, Footwear" once with "120 economies" rather than once per country.

update_db.py calls build_catalog() after every build and stores the summary tree in a
`catalog` table inside data.db, so it ships to R2 with the database. The API serves
the tree from there (/catalog) and expands a node on demand (/catalog/series) with a
query on the live `series` table.
"""
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

TOPIC_LABELS_PATH = Path(__file__).parent / 'topic_labels.json'

ECONOMY_NAMES = {'cn': 'China', 'jp': 'Japan', 'kr': 'Korea', 'tw': 'Taiwan', 'region': 'Regional & cross-country'}

FREQ_ORDER = 'dwmqa'

# ---------------------------------------------------------------------------
# Variant parsing
# ---------------------------------------------------------------------------

# Transform segments, stripped repeatedly from the end of a name.
_TRANSFORM = re.compile(r'''^(?:
      \d+[mqy]ma                                   # 3mma, 12mma, 4qma, 3yma
    | (?:Post-\d{4}\ )?z\ score                    # z score, Post-2005 z score
    | %\ (?:YoY|MoM|QoQ)(?:\ annualised)?(?:\ change)?
    | %\ (?:MoM|QoQ)\ saar
    | \d+M\ (?:chg|sum|average)
    | YoY\ chg | MoM\ chg | QoQ\ chg
    | YTD | SA | NSA | SAAR | Trend
    | Annual\ (?:average|sum) | Period\ average | Latest\ month
    | Contribution\ to\ %\ (?:YoY|MoM|QoQ)\ change | ppts
)$''', re.X)

_CCY = r'(?:USD|EUR|JPY|CNY|RMB|KRW|TWD|NTD|HKD|SGD|GBP|AUD|CAD|CHF|INR|IDR|THB|MYR|PHP|VND|BRL|MXN|RUB|TRY|ZAR|SAR|LCU|PPP\ USD|Int\ USD)'
# A single unit segment, stripped once after the transforms.
_UNIT = re.compile(rf'''^(?:
      {_CCY}(?:\ (?:bn|mn|tn|th|hundred\ mn))?(?:\ per\ .+)?(?:,\ real)?
    | (?:%|Percent)\ of\ .+ | %
    | Index | index | Index,\ .+ | RCA
    | Units?(?:\ (?:mn|th))? | Ton(?:ne)?s?\ (?:mn|th|bn)? | Tons?
    | (?:sqm|kg|kWh|GWh|TWh|barrels?|bbl)(?:\ (?:mn|th|bn))? | (?:bn|mn|th|tn)
    | Persons?(?:\ (?:mn|th))? | People(?:\ (?:mn|th))? | Hours | Days
    | \d{4}\ prices | size-adjusted | ppt | bp | bps
)$''', re.X)


def split_variant(name):
    """'China, CPI, Food, % YoY, 3mma' -> (['China', 'CPI', 'Food'], '% YoY, 3mma').

    The unit is stripped only when something is left afterwards, so 'Korea, Index'
    keeps a concept of its own.
    """
    segs = name.split(', ')
    tail = []
    while len(segs) > 1 and _TRANSFORM.match(segs[-1]):
        tail.insert(0, segs.pop())
    if len(segs) > 1 and _UNIT.match(segs[-1]):
        tail.insert(0, segs.pop())
    return segs, ', '.join(tail) or 'Level'


# ---------------------------------------------------------------------------
# Economy facet
# ---------------------------------------------------------------------------

_EXTRA_ECONOMIES = {
    'World', 'EU', 'EU27', 'EU28', 'Euro area', 'Eurozone', 'OECD', 'OECD total', 'G7', 'G20', 'ASEAN', 'APEC',
    'BRI', 'BRICS', 'Asia', 'East Asia', 'Emerging Asia', 'Developing Asia', 'Latin America', 'Africa', 'Europe',
    'Middle East', 'North America', 'Oceania', 'Rest of world', 'EM', 'DM',
    'Advanced economies', 'Emerging markets', 'Emerging and developing economies', 'African LDCs', 'LDCs',
    'US', 'USA', 'UK', 'Korea', 'South Korea', 'Taiwan', 'Hong Kong', 'Macau', 'Macao', 'Viet Nam', 'Vietnam',
    'Russia', 'Iran', 'Syria', 'Laos', 'Bolivia', 'Venezuela', 'Tanzania', 'Moldova', 'Czechia', 'Czech Republic',
    'Slovakia', 'Turkey', 'Türkiye', 'UAE', 'Saudi Arabia', 'North Korea', 'Brunei', 'Cape Verde', 'Ivory Coast',
    "Côte d'Ivoire", 'DR Congo', 'Congo', 'Micronesia', 'Palestine', 'Kosovo', 'China', 'Japan', 'Mainland China',
}


def _load_economies():
    names = set(_EXTRA_ECONOMIES)
    try:
        import pycountry
        for c in pycountry.countries:
            names.add(c.name)
            for attr in ('common_name', 'official_name'):
                if hasattr(c, attr):
                    names.add(getattr(c, attr))
    except ImportError:                      # the facet just finds fewer economies
        pass
    return names


ECONOMIES = _load_economies()

# A segment position is treated as the economy facet when at least this share of the
# topic's concepts have an economy there, and there are enough distinct ones to matter.
ECON_SHARE = 0.5
ECON_MIN = 5
ECON_SCAN = 4         # positions after the shared prefix to look at


MAX_ECON_SEGS = 3      # 'Europe, Northern America, Australia and New Zealand'


def learn_economies(rests, epos):
    """Economy names pycountry doesn't know, learnt from the shape of the path.

    Every path is read with 1..MAX_ECON_SEGS segments at the economy position taken as
    the economy, and the reading whose remaining path is shared by the most known
    economies wins. That picks up 'Hong Kong SAR' and 'Northern America' (same shape as
    'US, Exports'), and names that contain ', ' ('Yemen, Arab Republic', 'Congo, Rep.',
    'Bonaire, Sint Eustatius and Saba') — where the one-segment reading leaves a shape
    like 'Arab Republic, Exports' that only one or two other economies share.
    """
    shapes = defaultdict(set)
    for r in rests:
        if len(r) > epos + 1 and r[epos] in ECONOMIES:
            shapes[r[:epos] + r[epos + 1:]].add(r[epos])
    learnt = set()
    for r in rests:
        best_k, best_n = None, 1             # need at least two economies sharing the shape
        for k in range(1, MAX_ECON_SEGS + 1):
            if len(r) <= epos + k:
                break
            n = len(shapes.get(r[:epos] + r[epos + k:], ()))
            if n > best_n:
                best_k, best_n = k, n
        if best_k is not None:
            econ = ', '.join(r[epos:epos + best_k])
            if econ not in ECONOMIES:
                learnt.add(econ)
    return learnt


def pop_economy(path, epos, known):
    """Remove and return the economy segment(s) from a path (a list), or None.

    The topic's detected position is tried first, then the other leading positions,
    because some files mix layouts ('CEPII, ISIC V2, Korea, …' next to 'CEPII, Albania, …').
    Longer joins win, so 'Yemen, Arab Republic' is not read as 'Yemen'. At least one
    segment is always left to group on."""
    if epos is None:
        return None
    for p in [epos] + [i for i in range(ECON_SCAN) if i != epos]:
        for k in range(MAX_ECON_SEGS, 0, -1):
            if len(path) > p + k and ', '.join(path[p:p + k]) in known:
                econ = ', '.join(path[p:p + k])
                del path[p:p + k]
                return econ
    return None


def find_economy_pos(rests):
    best = None
    for p in range(ECON_SCAN):
        vals = [r[p] for r in rests if len(r) > p + 1]
        if not vals:
            break
        hits = [v for v in vals if v in ECONOMIES]
        distinct = len(set(hits))
        if len(hits) / len(rests) >= ECON_SHARE and distinct >= ECON_MIN:
            if best is None or distinct > best[1]:
                best = (p, distinct)
    return best[0] if best else None


# ---------------------------------------------------------------------------
# Topic helpers
# ---------------------------------------------------------------------------

def topic_of(source_file):
    """'/…/cn_input/cn_hh_survey_q.parquet' -> 'cn_hh_survey'."""
    stem = Path(source_file).stem
    return re.sub(r'_[dwmqa]$', '', stem)


def load_topic_labels():
    try:
        with open(TOPIC_LABELS_PATH, encoding='utf-8') as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def default_label(topic):
    words = topic.split('_')
    if words and words[0] in ECONOMY_NAMES:
        words = words[1:]
    return ' '.join(words).capitalize() or topic


def home_key(key, home):
    """Drop a leading home-economy segment ('Japan, …' in a Japan topic) so files that
    mix 'Japan, X' and 'EAE, Y' names group on X and Y rather than on 'Japan'."""
    return key[1:] if home and len(key) > 1 and key[0] == home else key


def common_prefix(lists):
    if not lists:
        return []
    first = lists[0]
    n = min(len(l) for l in lists) - 1      # always leave one segment to group on
    i = 0
    while i < n and all(l[i] == first[i] for l in lists):
        i += 1
    return first[:i]


def is_hidden(name, patterns):
    low = name.lower()
    return any(p in low for p in patterns)


def _freqs(s):
    return sorted(s, key=FREQ_ORDER.index)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

EXAMPLES = 4


def build_catalog(conn, hidden_patterns=()):
    """Summarise the series table into the catalogue tree (a JSON-able dict)."""
    hidden = [p.lower() for p in hidden_patterns]
    labels = load_topic_labels()

    # topic -> concept tuple -> {'variants': set, 'freqs': set, 'min', 'max', 'n'}
    topics = defaultdict(lambda: defaultdict(lambda: {'variants': set(), 'freqs': set(),
                                                      'min': None, 'max': None}))
    topic_country = {}
    topic_files = defaultdict(set)

    cur = conn.execute('SELECT name, country, frequency, source_file, min_date, max_date FROM series')
    for name, country, freq, src, lo, hi in cur:
        if hidden and is_hidden(name, hidden):
            continue
        t = topic_of(src or '')
        topic_country[t] = country
        topic_files[t].add(src)
        segs, variant = split_variant(name)
        c = topics[t][tuple(segs)]
        c['variants'].add(variant)
        c['freqs'].add(freq)
        if lo and (c['min'] is None or lo < c['min']):
            c['min'] = lo
        if hi and (c['max'] is None or hi > c['max']):
            c['max'] = hi

    economies = defaultdict(list)
    for t, concepts in topics.items():
        keys = list(concepts)
        home = ECONOMY_NAMES.get(topic_country[t]) if topic_country[t] != 'region' else None
        gkeys = [home_key(k, home) for k in keys]
        prefix = common_prefix(gkeys)
        rests = [k[len(prefix):] for k in gkeys]
        epos = find_economy_pos(rests)
        known = ECONOMIES | learn_economies(rests, epos) if epos is not None else ECONOMIES

        groups = {}
        t_freqs, t_econ, t_lo, t_hi, t_series = set(), set(), None, None, 0
        for key, rest in zip(keys, rests):
            c = concepts[key]
            path = list(rest)
            econ = pop_economy(path, epos, known)
            g = path[0]
            sg = path[1] if len(path) > 2 else None

            gn = groups.setdefault(g, {'name': g, 'concepts': 0, 'series': 0, 'freqs': set(),
                                       'economies': set(), 'min': None, 'max': None, 'sub': {}})
            nodes = [gn]
            if sg is not None:
                sn = gn['sub'].setdefault(sg, {'name': sg, 'concepts': 0, 'series': 0, 'freqs': set(),
                                               'economies': set(), 'min': None, 'max': None, 'examples': []})
                nodes.append(sn)
            leaf = nodes[-1]
            leaf.setdefault('examples', [])
            if len(leaf['examples']) < EXAMPLES and (econ is None or econ == _first_econ(leaf, econ)):
                leaf['examples'].append({'name': ', '.join(key),
                                         'variants': sorted(c['variants'], key=_variant_sort)[:8]})
            nv = len(c['variants'])
            for n in nodes:
                n['concepts'] += 1
                n['series'] += nv
                n['freqs'] |= c['freqs']
                if econ:
                    n['economies'].add(econ)
                n['min'] = _min(n['min'], c['min'])
                n['max'] = _max(n['max'], c['max'])
            t_freqs |= c['freqs']
            if econ:
                t_econ.add(econ)
            t_lo, t_hi = _min(t_lo, c['min']), _max(t_hi, c['max'])
            t_series += nv

        economies[topic_country[t]].append({
            'id': t,
            'label': labels.get(t) or default_label(t),
            'home': home,
            'prefix': ', '.join(prefix),
            'economy_pos': epos,
            'economy_extra': sorted(known - ECONOMIES) if epos is not None else [],
            'files': sorted(Path(f).name for f in topic_files[t]),
            'sources': sorted(topic_files[t]),
            'concepts': len(keys),
            'series': t_series,
            'freqs': _freqs(t_freqs),
            'economies': len(t_econ),
            'min': t_lo, 'max': t_hi,
            'groups': [_finish(g) for g in sorted(groups.values(), key=lambda g: -g['series'])],
        })

    out = []
    for code in ECONOMY_NAMES:
        ts = sorted(economies.get(code, []), key=lambda t: t['label'].lower())
        if not ts:
            continue
        out.append({'code': code, 'name': ECONOMY_NAMES[code], 'topics': ts,
                    'concepts': sum(t['concepts'] for t in ts),
                    'series': sum(t['series'] for t in ts)})
    return {'economies': out,
            'built': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
            'economy_names': sorted(ECONOMIES),
            'concepts': sum(e['concepts'] for e in out),
            'series': sum(e['series'] for e in out)}


def _first_econ(leaf, econ):
    """Examples for a faceted node all come from the first economy seen, so they read as a set."""
    if '_ex_econ' not in leaf:
        leaf['_ex_econ'] = econ
    return leaf['_ex_econ']


def _finish(node):
    out = {'name': node['name'], 'concepts': node['concepts'], 'series': node['series'],
           'freqs': _freqs(node['freqs']), 'economies': len(node['economies']),
           'min': node['min'], 'max': node['max']}
    if node.get('examples'):
        out['examples'] = node['examples']
    if node.get('sub'):
        out['sub'] = [_finish(s) for s in sorted(node['sub'].values(), key=lambda s: -s['series'])]
    return out


def _min(a, b):
    return b if a is None else a if b is None else min(a, b)


def _max(a, b):
    return b if a is None else a if b is None else max(a, b)


def _variant_sort(v):
    return (v != 'Level', v.count(','), v)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def store_catalog(conn, catalog):
    conn.execute('DROP TABLE IF EXISTS catalog')
    conn.execute('CREATE TABLE catalog (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
    conn.execute("INSERT INTO catalog VALUES ('tree', ?)", (json.dumps(catalog, separators=(',', ':')),))
    conn.commit()


PRIVATE_KEYS = ('sources', 'economy_extra')


def catalog_index(cat):
    """Economy -> topic -> group, without subgroups or examples (~0.5 MB): the first page load."""
    return {
        'built': cat.get('built'),
        'concepts': cat['concepts'], 'series': cat['series'],
        'economies': [{**{k: v for k, v in e.items() if k != 'topics'},
                       'topics': [{**{k: v for k, v in t.items() if k not in PRIVATE_KEYS + ('groups',)},
                                   'groups': [{k: v for k, v in g.items() if k not in ('sub', 'examples')}
                                              for g in t['groups']]}
                                  for t in e['topics']]}
                      for e in cat['economies']],
    }


def catalog_topic(cat, topic):
    """One topic in full: groups, subgroups and example concepts."""
    for e in cat['economies']:
        for t in e['topics']:
            if t['id'] == topic:
                return {**{k: v for k, v in t.items() if k not in PRIVATE_KEYS}, 'economy': e['code']}
    return None


def load_catalog(conn):
    try:
        row = conn.execute("SELECT value FROM catalog WHERE key = 'tree'").fetchone()
    except Exception:                       # older database without the table
        return None
    return json.loads(row[0]) if row else None


# ---------------------------------------------------------------------------
# Expanding a node
# ---------------------------------------------------------------------------

def node_series(conn, cat, topic, group, subgroup=None, economy=None, q=None, hidden_patterns=(),
                limit=100, offset=0):
    """Concepts under one catalogue node, with every full series name grouped by variant.

    The catalogue stores each topic's files and shared prefix, so this is one indexed
    scan of the topic's rows (source_file) filtered in Python by path.
    """
    t = next((t for e in cat['economies'] for t in e['topics'] if t['id'] == topic), None)
    if t is None:
        return None
    prefix = t['prefix'].split(', ') if t['prefix'] else []
    epos = t['economy_pos']
    # The build machine's economy list, not this one's: pycountry may be missing on the server
    known = set(cat.get('economy_names') or ECONOMIES) | set(t.get('economy_extra', []))
    hidden = [p.lower() for p in hidden_patterns]
    ql = q.lower() if q else None

    srcs = t['sources']
    rows = conn.execute(
        f"SELECT name, frequency, min_date, max_date FROM series WHERE source_file IN ({','.join('?' * len(srcs))})",
        srcs)

    concepts = {}
    econs = set()
    for name, freq, lo, hi in rows:
        if hidden and is_hidden(name, hidden):
            continue
        segs, variant = split_variant(name)
        gsegs = home_key(segs, t.get('home'))
        if gsegs[:len(prefix)] != prefix:
            continue
        path = gsegs[len(prefix):]
        econ = pop_economy(path, epos, known)
        if not path or path[0] != group:
            continue
        if subgroup is not None and (len(path) < 3 or path[1] != subgroup):
            continue
        if econ:
            econs.add(econ)
        if economy and econ != economy:
            continue
        concept = ', '.join(segs)
        if ql and ql not in name.lower():
            continue
        c = concepts.setdefault(concept, {'concept': concept, 'economy': econ, 'variants': {}})
        v = c['variants'].setdefault(variant, {'variant': variant, 'name': name, 'freqs': [],
                                               'min': lo, 'max': hi})
        v['freqs'].append(freq)
        v['min'], v['max'] = _min(v['min'], lo), _max(v['max'], hi)

    items = sorted(concepts.values(), key=lambda c: c['concept'])
    for c in items:
        vs = sorted(c['variants'].values(), key=lambda v: _variant_sort(v['variant']))
        for v in vs:
            v['freqs'] = _freqs(set(v['freqs']))
        c['variants'] = vs
    return {
        'topic': topic, 'group': group, 'subgroup': subgroup, 'economy': economy,
        'economies': sorted(econs),
        'total': len(items), 'offset': offset, 'limit': limit,
        'items': items[offset:offset + limit],
    }
