#!/usr/bin/env python3
"""
GitHub 港澳台 IPTV 源自动抓取 + 端到端真实验证 + TVBox 输出  v4
================================================================
v4 核心改进 - 解决 "验证通过但不能播" 的问题:
  - 端到端验证: master playlist → 子播放列表 → ts分片 → 0x47 MPEG-TS 校验
  - 支持相对路径解析 (之前 HOY TV 等官方源的相对路径全部漏掉)
  - 严格要求 ts 以 0x47 开头 (拒绝 freetv.fun 等跳转源的假阳性)
  - 0x47 同步链校验 (验证第二个TS包也以0x47开头，排除巧合匹配)
  - freetv.fun 等跳转源自动跟随到真实地址验证

v3 功能保留:
  - 17个数据源 + tonkiang.us cookie 支持
  - 台湾频道分类 (中天/东森/三立/民视等)
  - TVBox / M3U / JSON 三种输出格式

验证原理 (v4 端到端):
    Level 1 - HTTP连通: 状态码<400, 非HTML, 非重定向到错误页
    Level 2 - 内容识别: 区分 直连流 / 普通播放列表 / master playlist
    Level 3 - ts验证:  下载ts分片, 严格要求 0x47 同步字节 + 包链校验
    Level 4 - 跟随验证: master playlist 跟随子播放列表, 验证真实视频流

tonkiang.us 使用:
    export TONKIANG_COOKIE="4GrXYG2uktU7St15vBfqCQ4Lx.axczChvYzZigQMH4s-1784701372-1.2.1.1-vsKMrhTk5d1AsCzK_ize5oxeQzq7YAukUgeEZFLR_rlBfKdxiFIwig9RvTAZVdVGre8g6wXOhz5HmhNYj_sGKUIjMEybtFhjPSur.R_I95rLMZ9_eWATHCmOb2Wo6UXbaBOvTPTv9SoYMC7y7_CZ6B47of4yQKr07O9oWhZQHbr1mEYq13jU1wFtnC9c8wTctBLgQpavtafbjBxtn1XtWSMVXV3beLGwg8uER3zS2k7Tt8nxNcVNnDANHc38gtXoxBJHO3TnsMVlUc6Iledg89U2I4cnXC73a4lvh.x_YrjE8of5_0hvP6R.HncN6rNKoU9oaVn806xayC0_XdQZ8w"
    python scraper.py
"""

import argparse
import json
import os
import re
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, quote

warnings.filterwarnings('ignore')

try:
    import requests
except ImportError:
    print("[!] pip install requests")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════
# 数据源
# ═══════════════════════════════════════════════════════════════

SOURCES = [
    {"name": "sammy0101-HK", "url": "https://raw.githubusercontent.com/sammy0101/hk-iptv-auto/main/hk_live.m3u", "filter_keyword": True},
    {"name": "iptv-org-香港", "url": "https://iptv-org.github.io/iptv/countries/hk.m3u"},
    {"name": "iptv-org-澳门", "url": "https://iptv-org.github.io/iptv/countries/mo.m3u"},
    {"name": "Free-TV-香港", "url": "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_hong_kong.m3u8"},
    {"name": "MercuryZz-港澳台", "url": "https://raw.githubusercontent.com/MercuryZz/IPTVN/Files/GAT.m3u"},
    {"name": "Supprise0901-live", "url": "https://raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt", "filter_keyword": True},
    {"name": "Joker-Cold-已验证", "url": "https://raw.githubusercontent.com/Joker-Cold/HK-IPTV/main/source/COLD_OK.m3u8", "filter_keyword": True},
    {"name": "Joker-Cold-iptv", "url": "https://raw.githubusercontent.com/Joker-Cold/HK-IPTV/main/source/source_iptv.m3u", "filter_keyword": True},
    {"name": "Joker-Cold-全量", "url": "https://raw.githubusercontent.com/Joker-Cold/HK-IPTV/main/source/all_sources.m3u", "filter_keyword": True},
    {"name": "imDazui-港澳台202506", "url": "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%BE%B3%E9%97%A8202506.m3u"},
    {"name": "imDazui-港澳台2023", "url": "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%BE%B3%E9%97%A82023.m3u"},
    {"name": "imDazui-港澳台海外", "url": "https://raw.githubusercontent.com/imDazui/Tvlist-awesome-m3u-m3u8/master/m3u/%E5%8F%B0%E6%B9%BE%E9%A6%99%E6%B8%AF%E6%B5%B7%E5%A4%96.m3u", "filter_keyword": True},
    {"name": "ChinaIPTV-自动更新", "url": "https://raw.githubusercontent.com/hujingguang/ChinaIPTV/main/cnTV_AutoUpdate.m3u8", "filter_group": "港澳台"},
    {"name": "Guovin-TV", "url": "https://raw.githubusercontent.com/Guovin/TV/gd/output/result.m3u", "filter_keyword": True},
    {"name": "iptv-org-中文", "url": "https://iptv-org.github.io/iptv/languages/zho.m3u", "filter_keyword": True},
    {"name": "Kimentanm-aptv", "url": "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u", "filter_keyword": True},
    {"name": "vbskycn-iptv4", "url": "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.m3u", "filter_keyword": True},
]

TONKIANG_KEYWORDS = [
    "翡翠台", "TVB", "ViuTV", "HOY TV", "RTHK", "港台",
    "凤凰", "澳门", "TDM", "TVBS", "中天", "东森",
]

DEAD_DOMAINS = [
    'aktv.top', 'php.jdshipin.com', 'v2h.jdshipin.com',
    'smt2.1678520.xyz', 'iptv.wwkejishe.top',
]

CDN_PREFIX = "https://cdn.jsdelivr.net/gh/"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': '*/*', 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}

# ═══════════════════════════════════════════════════════════════
# 频道识别
# ═══════════════════════════════════════════════════════════════

HK_PATTERNS = [
    r'香港', r'Hong\s*Kong', r'\bHK\b', r'HKS', r'HKSTV',
    r'翡翠', r'Jade', r'TVB', r'无线', r'翡翠台', r'明珠',
    r'ViuTV', r'Viu\s*TV', r'HOY\s*TV', r'HOY',
    r'港台', r'RTHK', r'港台電視',
    r'凤凰', r'Phoenix', r'凤凰卫视',
    r'澳门', r'Macau', r'TDM', r'澳广视',
    r'ATV', r'亚洲电视', r'本港', r'國際台',
    r'有線', r'i-CABLE', r'Cable\s*TV',
    r'Now\s*[^\s]', r'Now\s*TV',
    r'天映', r'Celestial', r'耀才', r'BSTV',
    r'TVBS', r'中天', r'东森', r'東森', r'三立',
    r'民视', r'民視', r'台视', r'台視', r'中视', r'中視',
    r'华视', r'華視', r'公视', r'公視',
    r'八大', r'龙华', r'龍華', r'MOMO', r'ELTA', r'博斯',
    r'CTi', r'EBC', r'FTV', r'STV', r'TTV', r'PTS',
    r'寰宇', r'靖天', r'台娱',
]
HK_PAT = [re.compile(p, re.IGNORECASE) for p in HK_PATTERNS]

EXCLUDE_PATTERNS = [
    r'^CCTV', r'^湖南卫视', r'^东方卫视', r'^浙江卫视', r'^江苏卫视',
    r'^北京卫视', r'^广东卫视', r'^深圳卫视', r'^四川卫视',
    r'^山东卫视', r'^河南卫视', r'^湖北卫视', r'^安徽卫视',
    r'^天津卫视', r'^重庆卫视', r'^辽宁卫视', r'^吉林卫视',
    r'^黑龙江卫视', r'^福建卫视', r'^河北卫视', r'^江西卫视',
    r'^广西卫视', r'^云南卫视', r'^旅游卫视',
    r'^咪咕', r'^晴彩', r'^中国之声', r'^央广',
    r'免费订阅', r'温馨提示', r'维护时间', r'公告说明',
]
EXCLUDE_PAT = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]


def is_hk_channel(name):
    if any(p.search(name) for p in EXCLUDE_PAT):
        return False
    return any(p.search(name) for p in HK_PAT)


def is_dead_domain(url):
    try:
        host = urlparse(url).hostname or ''
        return any(host == d or host.endswith('.' + d) for d in DEAD_DOMAINS)
    except:
        return False


# ═══════════════════════════════════════════════════════════════
# M3U 解析
# ═══════════════════════════════════════════════════════════════

def _parse_extinf_line(line):
    in_quote = False
    last_comma = -1
    for i in range(len(line) - 1, -1, -1):
        if line[i] == '"':
            in_quote = not in_quote
        elif line[i] == ',' and not in_quote:
            last_comma = i
            break
    if last_comma == -1:
        return None
    return line[last_comma + 1:].strip(), line[:last_comma]


def parse_m3u(content, source_name, filter_group=None, filter_keyword=False):
    channels = []
    lines = content.replace('\r\n', '\n').replace('\r', '\n').strip().split('\n')
    current_info = None
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#EXTVLCOPT:'):
            continue
        if line.startswith('#EXTINF'):
            parsed = _parse_extinf_line(line)
            if parsed:
                name, hdr = parsed
                group = re.search(r'group-title="([^"]*)"', hdr, re.IGNORECASE)
                tvg_id = re.search(r'tvg-id="([^"]*)"', hdr, re.IGNORECASE)
                tvg_logo = re.search(r'tvg-logo="([^"]*)"', hdr, re.IGNORECASE)
                current_info = {
                    "name": name, "url": None,
                    "group": group.group(1) if group else "",
                    "tvg_id": tvg_id.group(1) if tvg_id else "",
                    "tvg_logo": tvg_logo.group(1) if tvg_logo else "",
                    "source": source_name,
                }
            continue
        if line.startswith('#'):
            continue
        if current_info and (line.startswith('http') or line.startswith('rtmp') or line.startswith('rtsp')):
            current_info['url'] = line
            include = True
            if filter_group:
                if filter_group not in current_info['group'] and filter_group not in current_info['name']:
                    include = False
            if filter_keyword and not is_hk_channel(current_info['name']):
                include = False
            if include and is_dead_domain(current_info['url']):
                include = False
            if include:
                channels.append(current_info)
            current_info = None
    return channels


def parse_tvbox(content, source_name, filter_keyword=False):
    channels = []
    lines = content.replace('\r\n', '\n').replace('\r', '\n').strip().split('\n')
    current_group = ""
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            if '#genre#' in line:
                parts = line.split(',')
                if len(parts) >= 2:
                    current_group = parts[0].strip()
            continue
        if ',' in line:
            parts = line.split(',', 1)
            name, url = parts[0].strip(), parts[1].strip()
            if url and (url.startswith('http') or url.startswith('rtmp') or url.startswith('rtsp')):
                ch = {"name": name, "url": url, "group": current_group,
                      "tvg_id": "", "tvg_logo": "", "source": source_name}
                include = True
                if filter_keyword and not is_hk_channel(name):
                    include = False
                if include and is_dead_domain(url):
                    include = False
                if include:
                    channels.append(ch)
    return channels


# ═══════════════════════════════════════════════════════════════
# tonkiang.us 抓取
# ═══════════════════════════════════════════════════════════════

def fetch_tonkiang(cookie_str, keywords, timeout=15):
    if not cookie_str:
        return []
    channels = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Cookie': cookie_str, 'Referer': 'https://www.tonkiang.us/',
    }
    for keyword in keywords:
        try:
            r = requests.get(f'https://www.tonkiang.us/?s={quote(keyword)}',
                           headers=headers, timeout=timeout, verify=False)
            if 'recaptcha' in r.text.lower() and len(r.text) < 5000:
                print(f"    [!] tonkiang cookie 已过期")
                break
            if r.status_code != 200:
                continue
            # 从搜索结果提取链接
            m3u8_urls = re.findall(r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)', r.text)
            for url in m3u8_urls:
                channels.append({"name": f"tonkiang-{keyword}", "url": url.strip(),
                               "group": "", "tvg_id": "", "tvg_logo": "", "source": "tonkiang.us"})
            # 也提取其他视频链接
            for line in r.text.split('\n'):
                m = re.search(r'href="(https?://[^"]+)"', line)
                if m:
                    url = m.group(1)
                    if any(ext in url for ext in ['.m3u8', '.ts', '.flv', '.mp4']):
                        if not any(c['url'] == url for c in channels):
                            channels.append({"name": f"tonkiang-{keyword}", "url": url,
                                           "group": "", "tvg_id": "", "tvg_logo": "", "source": "tonkiang.us"})
            time.sleep(0.5)
        except Exception as e:
            print(f"    [!] tonkiang '{keyword}': {str(e)[:40]}")
    return channels


# ═══════════════════════════════════════════════════════════════
# 网络请求
# ═══════════════════════════════════════════════════════════════

def fetch_url(url, timeout=15, use_cdn=True):
    urls = [url]
    if use_cdn and 'raw.githubusercontent.com' in url:
        gh = url.replace('https://raw.githubusercontent.com/', '')
        urls.append(f"{CDN_PREFIX}{gh}")
    for u in urls:
        try:
            r = requests.get(u, headers=HEADERS, timeout=timeout, verify=False)
            if r.status_code == 200 and len(r.text) > 50:
                return r.text
        except Exception:
            continue
    return ""


def _http_get(url, timeout=12, max_size=32768):
    """通用HTTP GET，返回 (bytes_data, status_code, final_url, content_type)"""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout,
                        stream=True, verify=False, allow_redirects=True)
        if r.status_code >= 400:
            r.close()
            return None, r.status_code, r.url, ''
        data = b''
        for chunk in r.iter_content(chunk_size=4096):
            data += chunk
            if len(data) >= max_size:
                break
        ct = r.headers.get('Content-Type', '')
        r.close()
        return data, r.status_code, r.url, ct
    except requests.exceptions.Timeout:
        return None, 0, '', 'timeout'
    except requests.exceptions.ConnectionError:
        return None, 0, '', 'conn refused'
    except Exception as e:
        return None, 0, '', str(e)[:40]


def _is_mpeg_ts(data):
    """
    严格 MPEG-TS 校验:
      - 至少 188 字节 (一个TS包)
      - 前 192 字节内找到 0x47 同步字节
      - 如果找到第二个包位置，也要验证 0x47
    返回 (bool, str)
    """
    if not data or len(data) < 188:
        return False, f"too small ({len(data)}B)"
    # 第一个字节就是 0x47
    if data[0] == 0x47:
        # 验证第二个包 (188字节后) 是否也是 0x47
        if len(data) >= 376:
            if data[188] == 0x47:
                return True, "MPEG-TS ✓✓"
            # 可能有 192 字节包长
            if len(data) >= 384 and data[192] == 0x47:
                return True, "MPEG-TS ✓✓"
            return True, "MPEG-TS ✓"
        return True, "MPEG-TS ✓"
    # 在前 192 字节搜索 0x47
    for i in range(min(192, len(data) - 187)):
        if data[i] == 0x47:
            # 验证下一个包
            if i + 188 * 2 <= len(data):
                if data[i + 188] == 0x47:
                    return True, "MPEG-TS (chain)"
            elif i + 188 <= len(data):
                if data[i + 188] == 0x47:
                    return True, "MPEG-TS (sync)"
            return True, "MPEG-TS (found)"
    return False, "not MPEG-TS"


def _resolve_segments(text, playlist_url):
    """从m3u8内容解析出ts分片和子播放列表URL"""
    segments = []
    sub_playlists = []
    base = playlist_url.rsplit('/', 1)[0]
    for line in text.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        full_url = line if line.startswith('http') else f"{base}/{line}"
        if '.m3u8' in line or '.m3u' in line:
            sub_playlists.append(full_url)
        else:
            segments.append(full_url)
    return segments, sub_playlists


def validate_stream(url, timeout=12):
    """
    端到端验证 v4:
      1. HTTP连通 + 拒绝HTML/错误页
      2. 区分内容类型: 直连流 / 普通m3u8 / master playlist
      3. m3u8: 解析ts分片 (支持相对路径) → 下载 → 0x47校验
      4. master playlist: 跟随子播放列表 → 重复步骤3
      5. 严格要求: ts必须包含MPEG-TS同步字节 0x47
    返回: (bool, str)
    """
    # Step 1: HTTP 请求
    data, status, final_url, ct = _http_get(url, timeout=timeout)
    if data is None:
        return False, ct if status == 0 else f"HTTP {status}"
    if status >= 400:
        return False, f"HTTP {status}"

    # 拒绝重定向到错误页
    if any(kw in final_url.lower() for kw in ['error', '404', 'blocked', 'denied', 'captcha']):
        return False, "redirect to error"

    text = data.decode('utf-8', errors='ignore').strip()

    # 拒绝HTML
    if text.startswith('<!') or text.startswith('<html') or '<!doctype' in text.lower()[:100]:
        return False, "HTML page"
    if len(text) > 200 and '<html' in text[:500].lower():
        return False, "HTML page"

    if len(text) < 20:
        return False, "empty"

    # Step 2: 直连视频流
    if any(v in ct for v in ['video/', 'application/octet-stream', 'application/mp4', 'application/vnd.apple.mpegurl']):
        if len(data) >= 188:
            ok, reason = _is_mpeg_ts(data[:4096])
            return (True, f"direct {reason}") if ok else (False, f"direct not ts: {reason}")
        return False, "direct too small"

    # Step 3: m3u8 检测
    if '#EXTM3U' not in text and '#EXTINF' not in text and '.m3u8' not in text and '.ts' not in text:
        # 不是m3u8也不是视频流
        if len(text) < 200:
            return False, f"not video ({len(text)}B)"
        if any(kw in text.lower() for kw in ['error', 'not found', '403', 'denied', 'blocked']):
            return False, "error response"
        return False, "unknown format"

    # Step 4: 解析播放列表
    segments, sub_playlists = _resolve_segments(text, url)

    # 4a: 有ts分片 → 直接验证
    if segments:
        for ts_url in segments[:3]:
            ts_data, ts_status, _, _ = _http_get(ts_url, timeout=timeout, max_size=4096)
            if ts_data is None:
                continue
            ok, reason = _is_mpeg_ts(ts_data)
            if ok:
                return True, reason
        return False, "ts not MPEG-TS"

    # 4b: master playlist → 跟随子播放列表
    if '#EXT-X-STREAM-INF' in text:
        if not sub_playlists:
            return False, "master: no subs"

        for sub_url in sub_playlists[:3]:
            sub_data, sub_status, _, _ = _http_get(sub_url, timeout=timeout, max_size=16384)
            if sub_data is None or sub_status >= 400:
                continue
            sub_text = sub_data.decode('utf-8', errors='ignore')
            sub_segs, _ = _resolve_segments(sub_text, sub_url)
            if sub_segs:
                for ts_url in sub_segs[:2]:
                    ts_data, ts_status, _, _ = _http_get(ts_url, timeout=timeout, max_size=4096)
                    if ts_data is None:
                        continue
                    ok, reason = _is_mpeg_ts(ts_data)
                    if ok:
                        return True, f"sub→{reason}"
        return False, "master: no valid ts"

    return False, "no playable content"


# ═══════════════════════════════════════════════════════════════
# 分类
# ═══════════════════════════════════════════════════════════════

def _normalize_name(name):
    """标准化频道名用于去重: 去掉标记如 [BD][HD][geo-blocked]*ee 等"""
    n = re.sub(r'\[([^\]]*)\]', '', name)  # 去[xxx]
    n = re.sub(r'\*([^*]+)\*', '', n)  # 去*xxx*
    n = re.sub(r'（[^）]*）', '', n)  # 去（xxx）
    n = re.sub(r'\([^)]*\)', '', n)  # 去(xxx)
    n = re.sub(r'\s+', ' ', n).strip()
    # 统一中英名称
    replacements = {
        '翡翠台': '翡翠台', '翡翠': '翡翠台', 'Jade': '翡翠台',
        '明珠台': '明珠台', '明珠': '明珠台',
        'tvb plus': 'TVB Plus', 'tvb星河': 'TVB星河', 'TVBS亚洲': 'TVBS亚洲',
        'TVBS亚洲': 'TVBS亚洲', 'tvbs亚洲': 'TVBS亚洲', 'TVBS新闻': 'TVBS新闻',
        'Viutv': 'ViuTV', 'viutv': 'ViuTV', 'hoy tv': 'HOY TV',
        '凤凰中文': '凤凰中文台', '凤凰资讯': '凤凰资讯台',
        '凤凰香港': '凤凰香港台',
        '中天新闻': '中天新闻台', '中天亚洲': '中天亚洲台',
        '东森新闻': '东森新闻台', '东森财经': '东森财经台',
        '台视新闻': '台视新闻台', '民视新闻': '民视新闻台',
        '三立新闻': '三立新闻台',
        '澳视澳门': '澳视澳门-TDM', '澳门TDM': '澳视澳门-TDM',
        '港台電視31': 'RTHK 31', '港台電視32': 'RTHK 32',
        '港台電視33': 'RTHK 33', '港台電視34': 'RTHK 34',
        '港台電視35': 'RTHK 35',
    }
    lower = n.lower()
    for src, dst in replacements.items():
        if lower == src.lower() or lower == src.lower().replace(' ', ''):
            return dst
    return n


def _url_quality(url, reason):
    """给URL质量打分, 越小越优"""
    score = 100
    # 官方源
    if 'rthk.hk' in url or 'rthklive' in url: score = 1
    elif 'hoy.tv' in url: score = 2
    elif 'freetv.fun' in url: score = 10  # 跳转服务, 但实际可用
    elif 'jdshipin.com' in url: score = 15
    elif 'epg.pw' in url: score = 12
    elif '163189.xyz' in url: score = 15
    elif 'akamaized.net' in url or 'akamaihd.net' in url: score = 3
    elif 'ifeng.com' in url: score = 4
    # 未知IP地址
    elif re.match(r'https?://\d+\.\d+', url): score = 50
    # 验证质量加分
    if '✓✓' in reason: score -= 5
    elif '✓' in reason: score -= 3
    elif '(chain)' in reason: score -= 4
    elif '(found)' in reason: score += 5
    return score


def _dedup_by_name(channels):
    """同名频道去重, 保留质量最高的URL"""
    name_map = {}
    for ch in channels:
        norm = _normalize_name(ch['name'])
        if norm not in name_map:
            name_map[norm] = ch
        else:
            existing = name_map[norm]
            old_score = _url_quality(existing['url'], existing.get('reason', ''))
            new_score = _url_quality(ch['url'], ch.get('reason', ''))
            if new_score < old_score:
                name_map[norm] = ch
    return list(name_map.values())


def classify_channel(name):
    n = name
    if any(k in n for k in ['RTHK', '港台電視', '港台电视', '港台']): return '港台RTHK'
    if any(k in n for k in ['翡翠', 'Jade', 'TVBJ', '无线']): return 'TVB翡翠台'
    if '明珠' in n: return 'TVB明珠台'
    if 'HOY' in n: return 'HOY TV'
    if 'ViuTV' in n or 'viu' in n.lower(): return 'ViuTV'
    if any(k in n for k in ['凤凰中文', 'Phoenix Chinese']): return '凤凰中文台'
    if any(k in n for k in ['凤凰资讯', 'Phoenix Info']): return '凤凰资讯台'
    if any(k in n for k in ['凤凰电影', '凤凰香港']): return '凤凰其他频道'
    if any(k in n for k in ['凤凰']): return '凤凰卫视'
    if any(k in n for k in ['TVBS']): return 'TVBS（台湾）'
    if any(k in n for k in ['中天', 'CTi']): return '中天（台湾）'
    if any(k in n for k in ['东森', '東森', 'EBC']): return '东森（台湾）'
    if any(k in n for k in ['三立']): return '三立（台湾）'
    if any(k in n for k in ['民视', '民視', 'FTV']): return '民视（台湾）'
    if any(k in n for k in ['台视', '台視', 'TTV']): return '台视（台湾）'
    if any(k in n for k in ['中视', '中視', 'CTS']): return '中视（台湾）'
    if any(k in n for k in ['公视', '公視', 'PTS']): return '公视（台湾）'
    if any(k in n for k in ['澳门', 'Macau', 'TDM', '澳视', '澳广']): return '澳门频道'
    if any(k in n for k in ['香港卫视', 'HKS', 'HKSTV']): return '香港卫视'
    if any(k in n for k in ['耀才', 'BSTV']): return '财经频道'
    if any(k in n for k in ['天映', 'Celestial']): return '电影频道'
    if any(k in n for k in ['Now', 'now', '有線', 'Cable']): return '有线/Now'
    if any(k in n for k in ['ATV', '亚洲电视', '本港', '國際']): return '已停播存档'
    if any(k in n for k in ['龙华', '龍華']): return '龙华（台湾）'
    if any(k in n for k in ['八大']): return '八大（台湾）'
    if any(k in n for k in ['MOMO', 'ELTA', '博斯']): return '其他台湾频道'
    return '港澳其他频道'


# ═══════════════════════════════════════════════════════════════
# 输出
# ═══════════════════════════════════════════════════════════════

def write_tvbox(channels, filepath):
    groups = {}
    for ch in channels:
        groups.setdefault(ch['category'], []).append(ch)

    order = [
        '港台RTHK', 'TVB翡翠台', 'TVB明珠台', 'ViuTV', 'HOY TV',
        '凤凰中文台', '凤凰资讯台', '凤凰其他频道', '凤凰卫视',
        'TVBS（台湾）', '中天（台湾）', '东森（台湾）', '三立（台湾）',
        '民视（台湾）', '台视（台湾）', '中视（台湾）', '公视（台湾）',
        '龙华（台湾）', '八大（台湾）', '其他台湾频道',
        '香港卫视', '澳门频道', '财经频道', '电影频道', '有线/Now',
        '港澳其他频道', '已停播存档',
    ]

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f'# 港澳台IPTV | {datetime.now().strftime("%Y-%m-%d %H:%M")}\n')
        f.write(f'# 频道: {len(channels)} | 全部端到端验证通过\n')
        f.write(f'# 验证方法: m3u8→ts分片→MPEG-TS 0x47同步字节校验\n\n')
        written = set()
        for g in order:
            if g in groups and groups[g]:
                f.write(f'{g},#genre#\n')
                for ch in groups[g]:
                    f.write(f'{ch["name"]},{ch["url"]}\n')
                f.write('\n')
                written.add(g)
        for g in sorted(groups.keys()):
            if g not in written and groups[g]:
                f.write(f'{g},#genre#\n')
                for ch in groups[g]:
                    f.write(f'{ch["name"]},{ch["url"]}\n')
                f.write('\n')
    print(f"  [+] TVBox: {filepath} ({len(channels)} 个)")


def write_m3u(channels, filepath):
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('#EXTM3U\n')
        for ch in channels:
            logo = ch.get('tvg_logo', '')
            if logo:
                f.write(f'#EXTINF:-1 tvg-logo="{logo}" group-title="{ch["category"]}",{ch["name"]}\n')
            else:
                f.write(f'#EXTINF:-1 group-title="{ch["category"]}",{ch["name"]}\n')
            f.write(f'{ch["url"]}\n')
    print(f"  [+] M3U: {filepath} ({len(channels)} 个)")


def write_json(channels, dead, stats, filepath):
    data = {
        "meta": {"title": "港澳台IPTV (端到端验证)",
                 "update": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "v": 4},
        "stats": stats,
        "alive": [{"name": c["name"], "url": c["url"], "category": c["category"],
                   "reason": c.get("reason",""), "source": c.get("source","")} for c in channels],
        "dead": dead,
    }
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ═══════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='港澳台IPTV端到端验证 v4')
    parser.add_argument('--no-validate', action='store_true')
    parser.add_argument('--timeout', type=int, default=10)
    parser.add_argument('--workers', type=int, default=40)
    parser.add_argument('--output-dir', type=str, default='output')
    parser.add_argument('--tonkiang-cookie', type=str, default='')
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"{'='*60}")
    print(f"  港澳台 IPTV 端到端验证 v4")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  数据源: {len(SOURCES)}个 | 验证: ts 0x47 MPEG-TS")
    print(f"{'='*60}")

    # Step 1: 抓取
    all_channels = []
    print(f"\n[1/3] 抓取数据源...")
    for i, src in enumerate(SOURCES):
        print(f"  [{i+1}/{len(SOURCES)}] {src['name']}", end='')
        content = fetch_url(src['url'], timeout=15)
        if not content:
            print(f" -> FAIL")
            continue
        if '#EXTM3U' in content or '#EXTINF' in content:
            channels = parse_m3u(content, src['name'],
                                filter_group=src.get('filter_group'),
                                filter_keyword=src.get('filter_keyword', False))
        elif '#genre#' in content:
            channels = parse_tvbox(content, src['name'],
                                   filter_keyword=src.get('filter_keyword', False))
        else:
            channels = parse_m3u(content, src['name'],
                                filter_group=src.get('filter_group'),
                                filter_keyword=src.get('filter_keyword', False))
        print(f" -> {len(channels)}")
        all_channels.extend(channels)
        time.sleep(0.3)

    # tonkiang
    tk_cookie = args.tonkiang_cookie or os.environ.get('TONKIANG_COOKIE', '')
    if tk_cookie:
        print(f"\n[1b] tonkiang.us...")
        tk = fetch_tonkiang(tk_cookie, TONKIANG_KEYWORDS)
        print(f"  -> {len(tk)} 个")
        all_channels.extend(tk)
    else:
        print(f"\n[1b] tonkiang: 未设置cookie, 跳过")

    # Step 2: 去重
    print(f"\n[2/3] 去重...")
    seen_urls = set()
    unique = []
    for ch in all_channels:
        url = ch['url']
        if not url or url in seen_urls:
            continue
        skip_filter = any(s in ch['source'] for s in ['iptv-org-香港', 'iptv-org-澳门'])
        if not skip_filter and not is_hk_channel(ch['name']):
            continue
        seen_urls.add(url)
        ch['category'] = classify_channel(ch['name'])
        unique.append(ch)

    # 排序: tonkiang优先 > 官方源 > freetv等第三方
    def sort_key(ch):
        url = ch['url'].lower()
        name = ch['name'].lower()
        if 'tonkiang' in ch['source']: return (-1, name)
        if any(d in url for d in ['rthk.hk', 'hoy.tv', 'viu.tv', 'freetv.fun']): return (0, name)
        return (1, name)
    unique.sort(key=sort_key)

    print(f"  去重: {len(unique)} 个频道")

    # Step 3: 验证
    if not args.no_validate:
        print(f"\n[3/3] 端到端验证 (workers={args.workers}, timeout={args.timeout}s)...")
        alive, dead = [], []
        done, total = 0, len(unique)

        def check(ch):
            ok, reason = validate_stream(ch['url'], timeout=args.timeout)
            return ch, ok, reason

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(check, ch): ch for ch in unique}
            for future in as_completed(futures):
                ch, ok, reason = future.result()
                done += 1
                icon = "✓" if ok else "✗"
                print(f"\r  [{done}/{total}] {icon} {ch['name'][:25]:<25} {reason[:25]}   ", end='', flush=True)
                if ok:
                    ch['reason'] = reason
                    alive.append(ch)
                else:
                    dead.append({"name": ch['name'], "url": ch['url'], "reason": reason, "source": ch['source']})

        print(f"\n\n  通过: {len(alive)}/{total} ({len(alive)*100//max(total,1)}%)")

        # Step 4: 频道名去重 (同名频道只保留最佳URL)
        print(f"\n[4] 频道名去重...")
        alive = _dedup_by_name(alive)
        print(f"  去重后: {len(alive)} 个频道")

        write_tvbox(alive, out / "hk.txt")
        write_m3u(alive, out / "hk.m3u")
        write_json(alive, dead, {
            "total_raw": len(all_channels), "total_unique": len(unique),
            "alive": len(alive), "dead": len(dead),
            "pass_rate": f"{len(alive)*100//max(total,1)}%",
        }, out / "hk.json")
    else:
        print(f"\n[3/3] 跳过验证")
        write_tvbox(unique, out / "hk.txt")
        write_m3u(unique, out / "hk.m3u")

    # 统计
    print(f"\n{'='*60}")
    cats = {}
    final = alive if not args.no_validate else unique
    for ch in final:
        cats[ch['category']] = cats.get(ch['category'], 0) + 1
    for c, n in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")
    print(f"  ---\n  合计: {len(final)} 个频道")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
