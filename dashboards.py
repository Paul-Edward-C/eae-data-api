"""
Live dashboards: access rules, the dashboard bundle, and note previews.

The dashboards (eastasiaecon.com/jp/, /jp/prices/ ...) are pages of the Ghost theme that draw from this API.
chartsite's build_dashboard.py writes every page's data into one gzipped bundle, publish.sh uploads it to R2
next to the chart-pack bundle (key DASH_R2_KEY), and this module serves each page only to readers whose
access level reaches it. Like the pack bundle it is never committed here: this repository is public.

Access levels (api._determine_access), lowest first:

    free     anonymous visitors and free members: nothing here (the members page has its own taste)
    daily    "Daily" (and the legacy "Individual" plan): every economy's FRONT page, plus the Region front page
    premium  "Daily + Data": the front pages and every topic page
    full     "East Asia" and admin: everything, and notes open in full on the site

A note opened from a dashboard: 'daily' and 'premium' readers get the excerpt and the note's charts from
/notes/{id} (fetched here with the Admin API, so the paywalled body never reaches them); 'full' readers are sent
to the post itself.
"""
import gzip
import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Optional

import sqlite_data_access as sda

ACCESS_RANK = {'free': 0, 'daily': 1, 'premium': 2, 'full': 3}
ACCESS_LABEL = {'daily': 'Daily', 'premium': 'Daily + Data', 'full': 'East Asia'}
ECONOMIES = {'jp', 'kr', 'tw', 'cn', 'region'}


def required_access(economy: str, page: str) -> str:
    """Front pages ('index') are Daily; topic pages are Daily + Data."""
    return 'daily' if page == 'index' else 'premium'


def can_view(economy: str, page: str, access: Optional[str]) -> bool:
    return ACCESS_RANK.get(access or 'free', 0) >= ACCESS_RANK[required_access(economy, page)]


# --------------------------------------------------------------------------- bundle

BUNDLE_KEY = os.environ.get('DASH_R2_KEY', 'dashboards.json.gz')
BUNDLE_PATH = Path(os.environ.get('DASH_BUNDLE_PATH', sda.DB_PATH.parent / 'dashboards.json.gz'))
RETRY_AFTER = 300

_bundle = None               # {'built': str, 'pages': {'jp/index': data, 'jp/prices': data, ...}}
_lock = threading.RLock()
_last_attempt = 0.0


def _read_bundle(path: Path) -> dict:
    with gzip.open(path, 'rt', encoding='utf-8') as fh:
        doc = json.load(fh)
    pages = doc.get('pages')
    if not isinstance(pages, dict) or not pages:
        raise ValueError('dashboard bundle has no pages')
    for key, d in pages.items():
        if '/' not in key or not isinstance(d, dict) or 'tiles' not in d:
            raise ValueError(f'dashboard page {key} is malformed')
    return doc


def _download_bundle() -> None:
    import boto3
    from botocore.config import Config
    client = boto3.client(
        's3', endpoint_url=f'https://{sda.R2_ENDPOINT}',
        aws_access_key_id=os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='auto', config=Config(signature_version='s3v4'))
    BUNDLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = BUNDLE_PATH.with_name(BUNDLE_PATH.name + '.download')
    try:
        client.download_file(sda.R2_BUCKET, BUNDLE_KEY, str(tmp))
        _read_bundle(tmp)                    # a bad upload never replaces a good bundle
        os.replace(tmp, BUNDLE_PATH)
    finally:
        if tmp.exists():
            tmp.unlink()


def refresh_bundle() -> dict:
    global _bundle, _last_attempt
    with _lock:
        _last_attempt = time.time()
        _download_bundle()
        _bundle = _read_bundle(BUNDLE_PATH)
        return {'dashboard_pages': len(_bundle['pages']), 'dashboards_built': _bundle.get('built')}


def _get_bundle() -> Optional[dict]:
    global _bundle, _last_attempt
    if _bundle is not None:
        return _bundle
    with _lock:
        if _bundle is not None:
            return _bundle
        try:
            if BUNDLE_PATH.exists():
                _bundle = _read_bundle(BUNDLE_PATH)
                return _bundle
        except Exception as e:
            print(f'Dashboard bundle unreadable, re-downloading: {e}')
        if time.time() - _last_attempt < RETRY_AFTER:
            return None
        try:
            refresh_bundle()
        except Exception as e:
            print(f'Could not load the dashboard bundle from R2: {e}')
        return _bundle


def load_page(economy: str, page: str) -> Optional[dict]:
    if economy not in ECONOMIES or not re.fullmatch(r'[a-z0-9-]{1,40}', page or ''):
        return None
    b = _get_bundle()
    return (b or {}).get('pages', {}).get(f'{economy}/{page}')


def page_list() -> list:
    """Every page with the access it needs - the catalogue, safe to serve openly."""
    b = _get_bundle() or {'pages': {}}
    out = []
    for key in sorted(b['pages']):
        e, p = key.split('/', 1)
        need = required_access(e, p)
        out.append({'economy': e, 'page': p, 'title': b['pages'][key].get('title'),
                    'required_access': need, 'label': ACCESS_LABEL[need]})
    return out


# --------------------------------------------------------------------------- note previews

_note_cache = {}             # post id -> (time, preview)
NOTE_TTL = 600


def _images(post: dict) -> list:
    """The note's chart images, in order: gallery and image cards of the lexical body (the post's own charts),
    else <img> tags in the html; the feature image first if it is not repeated. Screenshots are skipped."""
    seen, out = set(), []

    def add(src, w=None, h=None, caption=''):
        if not src or 'screenshot' in src.lower():
            return
        stem = re.sub(r'(-\d+)+(?=\.\w+$)', '', src.rsplit('/', 1)[-1]).lower()
        if stem in seen:
            return
        seen.add(stem)
        out.append({'src': src, 'width': w, 'height': h, 'caption': re.sub(r'<[^>]+>', '', caption or '')})

    try:
        nodes = json.loads(post.get('lexical') or '{}').get('root', {}).get('children', [])
    except json.JSONDecodeError:
        nodes = []
    for n in nodes:
        if n.get('type') == 'gallery':
            for im in n.get('images', []):
                add(im.get('src'), im.get('width'), im.get('height'), im.get('caption', ''))
        elif n.get('type') == 'image':
            add(n.get('src'), n.get('width'), n.get('height'), n.get('caption', ''))
    if not out:
        for src in re.findall(r'<img[^>]+src="([^"]+)"', post.get('html') or ''):
            add(src)
    if post.get('feature_image') and not out:
        add(post['feature_image'])
    return out


def note_preview(post_id: str, admin_get) -> Optional[dict]:
    """Excerpt and charts of one published post. admin_get(path, params) -> json (api.py supplies it)."""
    if not re.fullmatch(r'[0-9a-f]{24}', post_id or ''):
        return None
    hit = _note_cache.get(post_id)
    if hit and time.time() - hit[0] < NOTE_TTL:
        return hit[1]
    j = admin_get(f'posts/{post_id}/', {'formats': 'lexical,html', 'include': 'tags'})
    posts = (j or {}).get('posts') or []
    if not posts or posts[0].get('status') != 'published':
        return None
    p = posts[0]
    prev = {'id': p['id'], 'title': p.get('title'), 'published_at': p.get('published_at'),
            'excerpt': (p.get('custom_excerpt') or p.get('excerpt') or '').strip(),
            'feature_image': p.get('feature_image'),
            'tags': [t.get('name') for t in p.get('tags', [])],
            'url': p.get('url'),
            'images': _images(p)}
    _note_cache[post_id] = (time.time(), prev)
    return prev
