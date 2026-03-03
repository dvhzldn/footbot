__version__ = "0.5"

import json
import os
import asyncio
import time
import logging
import unicodedata
import httpx
import random

from datetime import datetime, timezone
from urllib.parse import quote_plus
from bs4 import BeautifulSoup
from typing import Union, List
from collections import deque
from dotenv import load_dotenv

load_dotenv()


# Load Configuration
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, "r") as f:
        return json.load(f)


try:
    CONFIG = load_config()
except FileNotFoundError:
    print(
        "Error: config.json not found. Please create it based on the template config.json.example"
    )
    exit(1)
except json.JSONDecodeError:
    print("Error: config.json is not valid JSON.")
    exit(1)

# Global settings
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
]
DETAILS_SEMAPHORE = asyncio.Semaphore(CONFIG["settings"]["details_concurrency"])
REPLAY_SEMAPHORE = asyncio.Semaphore(CONFIG["settings"]["replay_concurrency"])
ALERT_QUEUE = asyncio.Queue(maxsize=CONFIG["settings"]["max_queue_size"])
DELETION_QUEUE = deque(maxlen=CONFIG["settings"]["max_queue_size"])
REPLAY_TRACKER = set()
TV_CACHE = {}
MATCH_STATES = {}
BOOT_TIME = time.time()
COLORS = CONFIG.get("colors", {})
DEFAULT_COLOR = COLORS.get("default", 0xFFFFFF)
LIVE_INTERVAL = CONFIG["settings"]["live_interval"]
IDLE_INTERVAL = CONFIG["settings"]["idle_interval"]
DISCORD_WORKERS = CONFIG["settings"]["discord_workers"]
DISCORD_SEND_DELAY = CONFIG["settings"]["send_delay"]

# Webhook mapping
WEBHOOKS = {key: os.getenv(val, "") for key, val in CONFIG["webhooks"].items()}

OTHER_WEBHOOK = WEBHOOKS.get("OTHER", "")
GOALS_WEBHOOK = WEBHOOKS.get("GOALS", "")
PREM_WEBHOOK = WEBHOOKS.get("PREM", "")
FPL_WEBHOOK = WEBHOOKS.get("FPL", "")
FIXTURES_WEBHOOK = WEBHOOKS.get("FIXTURES", "")
SCOT_WEBHOOK = WEBHOOKS.get("SCOT", "")
UEFA_WEBHOOK = WEBHOOKS.get("UEFA", "")
EURO_WEBHOOK = WEBHOOKS.get("EURO", "")
LATAM_WEBHOOK = WEBHOOKS.get("LATAM", "")

LEAGUES = list(CONFIG["leagues"].keys())
LEAGUE_CONFIG = {
    l_id: {"webhook": WEBHOOKS[cfg["webhook_key"]], "fpl": cfg["fpl"]}
    for l_id, cfg in CONFIG["leagues"].items()
}
FPL_TARGET_PLAYERS = CONFIG["players"]

# Async client
async_client = httpx.AsyncClient(
    timeout=httpx.Timeout(8.0, connect=5.0),
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
)


class DiscordHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)

            def redact_url(url):
                if url and "discord.com/api/webhooks/" in url:
                    parts = url.split("/")
                    if len(parts) > 2:
                        return "/".join(parts[:-1]) + "/REDACTED"
                return "Unknown Webhook"

            safe_webhook_info = redact_url(OTHER_WEBHOOK)
            title = f"Log: {record.levelname}"
            description = f"**Source:** `{safe_webhook_info}`\n```\n{log_entry}\n```"

            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.create_task(
                        enqueue_alert(
                            title=title,
                            description=description,
                            color=COLORS.get("log_entry", 0x34495E),
                            url=OTHER_WEBHOOK,
                        )
                    )
            except RuntimeError:
                print(f"[{record.levelname}] {log_entry}")

        except Exception:
            self.handleError(record)


logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(message)s", datefmt="%H:%M:%S")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

discord_handler = DiscordHandler()
discord_handler.setFormatter(formatter)
discord_handler.setLevel(logging.WARNING)
discord_handler.addFilter(lambda record: "Discord" not in record.getMessage())
logger.addHandler(discord_handler)

logging.getLogger("httpx").setLevel(logging.WARNING)


async def fetch_json_async(url: str, retries=2):
    for attempt in range(retries):
        try:
            r = await async_client.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except (httpx.ConnectError, httpx.ConnectTimeout):
            if attempt < retries - 1:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            logging.error(f"Fetch error for {url} after {retries} attempts")
        except Exception as e:
            logging.error(f"Fetch error for {url}\n\n{e}")
            break
    return {}


def normalize(name):
    return "".join(
        c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c)
    ).lower()


async def send_embed(
    title: str,
    description: str,
    color: int = CONFIG["colors"]["default"],
    url: Union[str, List[str]] = [OTHER_WEBHOOK],
    thumb: str | None = None,
    delete_after: int = 0,
):
    targets = [url] if isinstance(url, str) else list(set(url))

    embed = {
        "title": title,
        "description": description,
        "color": color,
    }

    if thumb and isinstance(thumb, str) and thumb.startswith("http"):
        embed["thumbnail"] = {"url": thumb}

    payload = {"embeds": [embed]}

    for target in targets:
        if not target or not isinstance(target, str):
            continue

        try:
            r = await async_client.post(f"{target}?wait=true", json=payload, timeout=10)

            if r.status_code == 429:
                retry_after = r.json().get("retry_after", 1)
                logging.warning(f"Rate limited. Retrying in {retry_after}s")
                await asyncio.sleep(retry_after)
                await async_client.post(f"{target}?wait=true", json=payload, timeout=10)
                continue

            r.raise_for_status()

            if delete_after > 0:
                msg_id = r.json().get("id")
                if msg_id:
                    DELETION_QUEUE.append(
                        {
                            "url": target,
                            "id": msg_id,
                            "at": time.time() + (delete_after * 3600),
                        }
                    )

        except httpx.HTTPStatusError as e:
            logging.error(f"Webhook Error {e.response.status_code} for {title}")
        except Exception as e:
            logging.error(f"Network error sending {title}: {e}")


async def enqueue_alert(
    title: str,
    description: str,
    color: int = CONFIG["colors"]["default"],
    url: Union[str, List[str]] = OTHER_WEBHOOK,
    thumb: str | None = None,
    delete_after: int = 0,
):
    if time.time() - BOOT_TIME < CONFIG["settings"]["boot_grace_period"]:
        return
    await ALERT_QUEUE.put(
        {
            "title": title,
            "description": description,
            "color": color,
            "url": url,
            "thumb": thumb,
            "delete_after": delete_after,
        }
    )
    await asyncio.sleep(CONFIG["settings"]["send_delay"])


async def discord_worker():
    while True:
        first = await ALERT_QUEUE.get()
        alerts = [first]

        try:
            while len(alerts) < 10:
                nxt = await asyncio.wait_for(ALERT_QUEUE.get(), timeout=0.1)
                alerts.append(nxt)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

        for alert in alerts:
            try:
                await send_embed(
                    title=alert.get("title"),
                    description=alert.get("description"),
                    url=alert.get("url", OTHER_WEBHOOK),
                    thumb=alert.get("thumb"),
                    delete_after=alert.get("delete_after", 0),
                )
                await asyncio.sleep(DISCORD_SEND_DELAY)
            except Exception as e:
                logging.error(f"Worker failed to process alert: {e}")
            finally:
                ALERT_QUEUE.task_done()


async def cleanup_worker():
    while True:
        try:
            now = time.time()
            remaining = deque()

            while DELETION_QUEUE:
                m = DELETION_QUEUE.popleft()

                if now >= m["at"]:
                    try:
                        delete_url = f"{m['url']}/messages/{m['id']}"
                        response = await async_client.delete(delete_url)

                        if response.status_code == 429:
                            retry_after = response.json().get("retry_after", 1)
                            m["at"] = now + retry_after
                            remaining.append(m)
                            logging.warning(
                                f"Rate limited during deletion. Retrying {m['id']} later."
                            )
                        elif response.status_code not in [204, 404]:
                            logging.warning(
                                f"Failed to delete {m['id']}: {response.status_code}"
                            )

                    except Exception as e:
                        logging.error(f"Error deleting message {m['id']}: {e}")
                else:
                    remaining.append(m)

            DELETION_QUEUE.extend(remaining)

        except Exception as e:
            logging.error(f"Cleanup worker loop error: {e}")

        await asyncio.sleep(CONFIG["settings"]["cleanup_interval"])


async def get_uk_tv():
    tv_map = {}
    try:
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        r = await async_client.get(
            CONFIG["endpoints"]["fixture_channels"], headers=headers, timeout=10
        )
        soup = BeautifulSoup(
            r.text,
            "html.parser",
        )
        for fix in soup.select("div.fixture"):
            teams, chan = fix.select_one("div.fixture__teams"), fix.select_one(
                "span.channel-pill"
            )
            if teams and chan:
                tv_map[normalize(teams.get_text())] = chan.get_text(strip=True)
    except:
        pass
    return tv_map


async def get_match_details(league, game_id, scoreboard_event=None):
    url = CONFIG["endpoints"]["event_summary"].format(league=league, game_id=game_id)
    data = await fetch_json_async(url)
    if not data or not isinstance(data, dict):
        return {}

    header = data.get("header", {})
    competitions = header.get("competitions", [{}])
    live_clock = competitions[0].get("status", {}).get("displayClock", "??")

    lineup_text, tracked = "", []

    for r in data.get("rosters", []):
        t_info = r.get("team", {})
        t_name = t_info.get("displayName", "Team")
        roster = r.get("roster", [])

        starters = [
            f"{p.get('jersey','??'):<3}{p.get('athlete',{}).get('displayName')}"
            for p in roster
            if p.get("starter")
        ]
        subs = [
            f"{p.get('jersey','??'):<3}{p.get('athlete',{}).get('displayName')}"
            for p in roster
            if not p.get("starter")
        ]

        if len(starters) >= 11:
            lineup_text += (
                f"**{t_name}** Line-up:\n```\n" + "\n".join(starters) + "```\n"
            )
            if subs:
                lineup_text += f"Subs:\n```\n" + "\n".join(subs) + "```\n\n"

    all_goals, all_cards, match_events = {}, {}, []
    for detail in data.get("keyEvents", []):
        evt_text = detail.get("type", {}).get("text", "").lower()
        evt_type = detail.get("type", {}).get("type", "").lower()
        evt_id = detail.get("id", "")
        t_str = detail.get("clock", {}).get("displayValue", "??")
        scoring_play = detail.get("scoringPlay", False)
        tid = str(detail.get("team", {}).get("id", ""))
        p_names = [
            p.get("athlete", {}).get("displayName", "Unknown")
            for p in detail.get("participants", [])
        ]
        p_primary = p_names[0] if p_names else "Unknown"
        evt_wallclock = detail.get("wallclock", datetime.now)

        evt_time = (
            datetime.strptime(evt_wallclock, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )

        ids = [
            str(p.get("athlete", {}).get("id", ""))
            for p in detail.get("participants", [])
        ] + p_names

        if scoring_play:
            all_goals.setdefault(tid, []).append(f"{p_primary} ({t_str})")
            match_events.append(
                {
                    "type": "GOAL",
                    "scorer": p_primary,
                    "clock": t_str,
                    "team_id": tid,
                    "identifiers": ids,
                    "full_text": detail.get("text", ""),
                    "event_id": evt_id,
                    "event_time": evt_time,
                }
            )
        elif evt_type == "yellow-card":
            all_cards.setdefault(tid, []).append(f"🟨 {p_primary} ({t_str})")
            match_events.append(
                {
                    "type": "CARD",
                    "player": p_primary,
                    "card_type": "YELLOW",
                    "clock": t_str,
                    "team_id": tid,
                    "identifiers": ids,
                    "event_id": evt_id,
                    "event_time": evt_time,
                }
            )

        elif evt_type == "red-card":
            all_cards.setdefault(tid, []).append(f"🟥 {p_primary} ({t_str})")
            match_events.append(
                {
                    "type": "CARD",
                    "player": p_primary,
                    "card_type": "RED",
                    "clock": t_str,
                    "team_id": tid,
                    "identifiers": ids,
                    "event_id": evt_id,
                    "event_time": evt_time,
                }
            )

        elif evt_type == "substitution":
            match_events.append(
                {
                    "type": "SUB",
                    "on": p_primary,
                    "off": (p_names[1] if len(p_names) > 1 else "Unknown"),
                    "clock": t_str,
                    "team_id": tid,
                    "identifiers": ids,
                    "event_id": evt_id,
                }
            )

    snap, team_data = {}, {}
    box = data.get("boxscore", {})
    h_comps = header.get("competitions", [{}])[0].get("competitors", [])

    for comp in h_comps:
        t_info = comp.get("team", {})
        tid = str(t_info.get("id"))
        form = comp.get("form", "N/A")

        top_scorers = "N/A"
        for leader_cat in data.get("leaders", []):
            if str(leader_cat.get("team", {}).get("id")) == tid:
                top_list = [
                    f"{l.get('athlete',{}).get('displayName')} ({l.get('displayValue')})"
                    for l in leader_cat.get("leaders", [])
                ]
                top_scorers = ", ".join(top_list[:2])
                break

        s_map = {}
        if scoreboard_event:
            sb_comp = next(
                (
                    c
                    for c in scoreboard_event.get("competitions", [{}])[0].get(
                        "competitors", []
                    )
                    if str(c.get("id")) == tid
                ),
                {},
            )
            for s in sb_comp.get("statistics", []):
                s_map[s["name"]] = s.get("displayValue", "0")

        stats_list = next(
            (
                ts.get("statistics", [])
                for ts in box.get("teams", [])
                if str(ts.get("team", {}).get("id")) == tid
            ),
            [],
        )
        for s in stats_list:
            s_map[s["name"]] = s.get("displayValue", "0")

        corners = int(float(s_map.get("wonCorners", s_map.get("corners", 0))))
        shots = int(float(s_map.get("totalShots", 0)))
        sot = int(float(s_map.get("shotsOnTarget", 0)))
        poss = s_map.get("possessionPct", "0")

        snap[tid] = {
            "name": t_info.get("displayName"),
            "logo": (
                t_info.get("logos", [{}])[0].get("href") if t_info.get("logos") else ""
            ),
            "form": form,
            "top_scorers": top_scorers,
            "color": int(t_info.get("color", "3498DB"), 16),
            "corners": corners,
            "shots": shots,
            "sot": sot,
            "poss": f"{poss}%",
        }

        g_summary = (
            "⚽ " + ", ".join(all_goals.get(tid, [])) if tid in all_goals else ""
        )
        c_summary = ", ".join(all_cards.get(tid, [])) if tid in all_cards else ""
        team_data[tid] = {
            "name": snap[tid]["name"],
            "stats": (
                f"```\n{'Possession':<22}{snap[tid]['poss']:>4}\n"
                f"{'Shots':<22}{snap[tid]['shots']:>4}\n"
                f"{'Shots on target':<22}{snap[tid]['sot']:>4}\n"
                f"{'Corners':<22}{snap[tid]['corners']:>4}```"
            ),
            "summary": f"{g_summary}\n{c_summary}".strip(),
        }

    recap_data = data.get("article", {})
    recap_article = None
    if recap_data.get("type", "").lower() == "recap":
        recap_article = {
            "headline": recap_data.get("headline"),
            "description": recap_data.get("description"),
            "url": recap_data.get("links", {}).get("web", {}).get("href"),
            "image": (
                recap_data.get("images", [{}])[0].get("url")
                if recap_data.get("images")
                else None
            ),
        }

    return {
        "live_clock": live_clock,
        "lineups": lineup_text,
        "snapshot": snap,
        "events": match_events,
        "team_results": team_data,
        "article": recap_article,
        "goals_summary": "\n".join(
            [
                f"{snap[t]['name']}: {', '.join(g)}"
                for t, g in all_goals.items()
                if t in snap
            ]
        ),
        "cards_summary": "\n".join(
            [
                f"{snap[t]['name']}: {', '.join(c)}"
                for t, c in all_cards.items()
                if t in snap
            ]
        ),
    }


async def fetch_league_data(l_code: str, retries: int = 3):
    url = CONFIG["endpoints"]["scoreboard"].format(l_code=l_code)
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    for attempt in range(retries):
        try:
            response = await async_client.get(url, headers=headers, timeout=15)
            response.raise_for_status()

            data = response.json()
            if "events" in data:
                return l_code, data

        except (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.RemoteProtocolError,
        ) as e:
            if attempt < retries - 1:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            logging.error(
                f"Connection Error for {l_code} after {retries} attempts: {e}"
            )

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429 and attempt < retries - 1:
                await asyncio.sleep(5)
                continue
            logging.error(f"API Error for {l_code}: {e.response.status_code}")
            break

        except Exception as e:
            content_preview = (
                response.text[:100] if "response" in locals() else "No response"
            )
            logging.error(
                f"Unexpected Error for {l_code}: {e} | "
                f"Content type: {response.headers.get('Content-Type')} | "
                f"Body start: {content_preview}"
            )
            break

    return l_code, {}


async def handle_replay_task(home, away, score_txt, scorer, goal_ts):
    async with REPLAY_SEMAPHORE:
        await asyncio.sleep(CONFIG["settings"]["replay_delay"])

        unique_key = f"{home}_{away}_{score_txt}".replace(" ", "")
        if unique_key in REPLAY_TRACKER:
            return

        h_keywords = [k for k in normalize(home).split() if len(k) > 2]
        a_keywords = [k for k in normalize(away).split() if len(k) > 2]

        search_query = f"subreddit:{CONFIG['reddit']['search_subreddit']} {' '.join(h_keywords)} {' '.join(a_keywords)}"
        encoded_query = quote_plus(search_query)
        url = CONFIG["endpoints"]["goal_replays"].format(search_query=encoded_query)

        headers = {"User-Agent": os.getenv("REDDIT_USER_AGENT", "FootBot/1.0")}

        logging.info(
            f"[REPLAY] Task started for {home} vs {away} (Scorer: {scorer})\n{url}"
        )

        for attempt in range(8):
            try:
                r = await async_client.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    children = data.get("data", {}).get("children", [])
                    logging.info(
                        f"[REPLAY] Found {len(children)} posts for\n {score_txt}"
                    )

                    for post in children:
                        p = post.get("data", {})
                        title = p.get("title", "No Title")
                        post_time = p.get("created_utc", 0)
                        post_url = p.get("url", "").lower()

                        is_too_early = post_time < (goal_ts - 120)
                        is_too_late = post_time > (goal_ts + 600)

                        if is_too_early or is_too_late:
                            diff = post_time - goal_ts
                            logging.info(
                                f"[REPLAY] Time mismatch: {diff}s offset\n"
                                f"Window: -120 to +600\nTitle: {title}"
                            )
                            continue

                        if p.get("subreddit", "").lower() != "soccer":
                            continue

                        video_domains = CONFIG["reddit"]["video_domains"]
                        is_video = any(domain in post_url for domain in video_domains)

                        if is_video:
                            final_url = post_url

                            resp = await async_client.post(
                                GOALS_WEBHOOK,
                                json={
                                    "content": f"📺 {score_txt} ({scorer})\n\n{final_url}"
                                },
                            )
                            logging.info(
                                f"[REPLAY] Discord Response: {resp.status_code}"
                            )
                            REPLAY_TRACKER.add(unique_key)

                            return
                        else:
                            logging.info(
                                f"[REPLAY] Skipping '{title}' - Not a recognized video domain: {post_url}"
                            )

                elif r.status_code == 429:
                    reset_time = r.headers.get("x-ratelimit-reset", "unknown")
                    logging.warning(
                        f"[REPLAY] Reddit Rate Limit (429). Reset in {reset_time}s"
                    )
                    await asyncio.sleep(reset_time)
                else:
                    logging.error(
                        f"[REPLAY] Reddit API Error {r.status_code}: {r.text}"
                    )

            except Exception as e:
                logging.error(f"[REPLAY] Search Exception: {e}")

            await asyncio.sleep(CONFIG["settings"]["replay_retry"])

        logging.info(f"Replay not found\n{home} vs {away} (Scorer: {scorer})")


async def handle_match_events(
    gs, details, targets, league, score_txt, clock_str, scorers_txt
):
    goal_scored = False
    is_fpl_league = LEAGUE_CONFIG.get(league, {}).get("fpl", False)

    for evt in details.get("events", []):
        evt_id = evt.get("event_id")
        if evt_id in gs["processed_events"]:
            continue

        try:
            event_ts = details.get("event_time")
            if event_ts < (BOOT_TIME):
                gs["processed_events"].add(evt_id)
                continue
        except:
            if time.time() - BOOT_TIME < 15:
                gs["processed_events"].add(evt_id)
                continue

        team = details["snapshot"].get(
            evt["team_id"], {"name": "Match", "logo": "", "color": 0x3498DB}
        )

        if evt["type"] == "GOAL":
            goal_scored = True
            description = f"{evt.get('full_text', '')}\n\n{score_txt}"

            await enqueue_alert(
                f"[{evt['clock']}] {team['name']} scorer: {evt['scorer']}",
                description,
                color=team["color"],
                url=targets + [GOALS_WEBHOOK],
                thumb=team["logo"],
                delete_after=3,
            )

            team_ids = list(details["snapshot"].keys())
            if len(team_ids) >= 2:
                h_name = details["snapshot"][team_ids[0]]["name"]
                a_name = details["snapshot"][team_ids[1]]["name"]
                asyncio.create_task(
                    handle_replay_task(
                        h_name, a_name, score_txt, evt["scorer"], time.time()
                    )
                )

        elif evt["type"] == "SUB":
            await enqueue_alert(
                f"{clock_str} 🔄 Sub: {team['name']}",
                f"**ON:** {evt['on']}\n**OFF:** {evt['off']}\n\n{score_txt}\n{scorers_txt}",
                color=team["color"],
                url=targets,
                thumb=team["logo"],
                delete_after=3,
            )

        elif evt["type"] == "CARD":
            emoji = "🟥" if evt["card_type"] == "RED" else "🟨"
            await enqueue_alert(
                f"{clock_str} {emoji} Card: {team['name']}",
                f"**{evt['player']}** {evt['clock']}\n\n{score_txt}\n{scorers_txt}",
                color=team["color"],
                url=targets,
                thumb=team["logo"],
                delete_after=3,
            )

        if is_fpl_league:
            matched_player = None
            for p_id, p_name in FPL_TARGET_PLAYERS.items():
                if p_id in evt.get("identifiers", []) or p_name in evt.get(
                    "identifiers", []
                ):
                    matched_player = p_name
                    break

            if matched_player:
                await enqueue_alert(
                    f"🎯 FPL Impact: {matched_player}",
                    f"Event: **{evt.get('short_text', evt['type'])}**\n{score_txt}\n{scorers_txt}",
                    color=COLORS.get("fpl_impact", 0x00FF00),
                    url=FPL_WEBHOOK,
                    thumb=team.get("logo"),
                    delete_after=72,
                )

        gs["processed_events"].add(evt_id)

    return goal_scored


async def handle_milestones(
    gs,
    details,
    event,
    targets,
    score_txt,
    scorers_txt,
    uk_tv,
    league_name,
    curr_score,
    h_team,
    a_team,
):
    status = event["status"]
    state = status["type"]["state"]
    period = status.get("period", 1)
    comp = event.get("competitions", [])[0]
    res = details.get("team_results", {})
    h_id, a_id = [str(t["team"]["id"]) for t in comp["competitors"]]

    if state != "pre" and not details.get("team_results"):
        return

    if state == "pre" and not gs["lineup"] and details.get("lineups"):
        tv = next(
            (
                v
                for k, v in uk_tv.items()
                if normalize(h_team) in k and normalize(a_team) in k
            ),
            "TBD",
        )

        h_snap, a_snap = details["snapshot"].get(h_id, {}), details["snapshot"].get(
            a_id, {}
        )
        lineup_parts = details["lineups"].split("\n\n")
        h_lineup = lineup_parts[0] if len(lineup_parts) > 0 else ""
        a_lineup = lineup_parts[1] if len(lineup_parts) > 1 else ""

        match_info = (
            f"**{h_team}**\n\n{league_name} form: ```{h_snap.get('form', '???')}```\n{h_lineup}\n\n"
            f"Top scorers:```{h_snap.get('top_scorers', 'N/A')}```\n"
            f"**{a_team}**\n\n{league_name} form: ```{a_snap.get('form', '???')}```\n{a_lineup}\n\n"
            f"Top scorers:```{a_snap.get('top_scorers', 'N/A')}```"
        )

        ko_dt = datetime.strptime(event["date"], "%Y-%m-%dT%H:%MZ").replace(
            tzinfo=timezone.utc
        )
        await enqueue_alert(
            f"{ko_dt.strftime('%H:%M')} {h_team} v {a_team}",
            f"{league_name}\n📺 TV: {tv}\n\n{match_info}",
            url=targets,
            delete_after=4,
        )
        gs["lineup"] = True

    if state == "in" and period == 1 and not gs["ko"]:
        await enqueue_alert(
            f"Kick-off: {h_team} v {a_team}", league_name, url=targets, delete_after=3
        )
        gs["ko"] = True

    if status["type"].get("description", "").lower() == "halftime" and not gs["ht"]:
        summary_msg = (
            f"{league_name}\n\n"
            f"**{res[h_id]['name']}**\n{res[h_id]['stats']}\n{res[h_id]['summary']}\n\n"
            f"**{res[a_id]['name']}**\n{res[a_id]['stats']}\n{res[a_id]['summary']}"
        )
        await enqueue_alert(
            f"Half-time: {h_team} {curr_score} {a_team}",
            summary_msg,
            url=targets,
            delete_after=3,
        )
        gs["ht"] = True

    if state == "in" and period == 2 and not gs["sh"]:
        await enqueue_alert(
            f"Second half: {h_team} {curr_score} {a_team}\n\n{scorers_txt}",
            league_name,
            url=targets,
            delete_after=3,
        )
        gs["sh"] = True

    if state == "post" and not gs["ft"]:
        match_start_ts = (
            datetime.strptime(event["date"], "%Y-%m-%dT%H:%MZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )

        if (time.time() - match_start_ts) < 14400:
            summary_msg = (
                f"{league_name}\n\n"
                f"**{res[h_id]['name']}**\n{res[h_id]['stats']}\n{res[h_id]['summary']}\n\n"
                f"**{res[a_id]['name']}**\n{res[a_id]['stats']}\n{res[a_id]['summary']}"
            )
            await enqueue_alert(
                f"Full-time: {h_team} {curr_score} {a_team}",
                summary_msg,
                url=targets,
                delete_after=12,
            )
        gs["ft"] = True


async def handle_commentary_alerts(gs, details, score_txt, scorers_txt):
    for comm in details.get("commentary", []):
        c_id = str(comm.get("sequence"))
        c_text = comm.get("text", "")

        if not c_id or c_id in gs["processed_commentary"]:
            continue

        play = comm.get("play") or {}
        c_type = play.get("type", {}).get("text", "Alert")

        for p_id, p_name in FPL_TARGET_PLAYERS.items():
            if p_name.lower() in c_text.lower():
                c_clock = comm.get("time", {}).get("displayValue", "??")
                await enqueue_alert(
                    f"[{c_clock}] {p_name} - {c_type}",
                    f"{c_text}*\n\n{score_txt}\n{scorers_txt}",
                    color=COLORS.get("fpl_impact", 0x00FF00),
                    url=FPL_WEBHOOK,
                    delete_after=72,
                )
                break

        gs["processed_commentary"].add(c_id)


async def handle_recap(gs, article, targets, score_txt, scorers_txt):
    if gs.get("recap_sent"):
        return

    await enqueue_alert(
        f"📝 Match Report: {score_txt}",
        f"{scorers_txt}\n\n### {article['headline']}\n{article['description']}\n\n[Read Full Report on ESPN]({article['url']})",
        color=COLORS.get("match_report", 0x0055AA),
        url=targets,
        thumb=article["image"],
        delete_after=12,
    )

    gs["recap_sent"] = True


async def update_match_details(
    league, game_id, event, gs, targets, score_txt, clock_str
):
    await asyncio.sleep(5)

    async with DETAILS_SEMAPHORE:
        try:
            details = await get_match_details(league, game_id, scoreboard_event=event)
            if details and details.get("snapshot"):
                gs["details"] = details
        except Exception as e:
            logging.error(f"Error fetching details for {game_id}: {e}")


async def process_match(league, event, league_name, uk_tv):
    is_startup = (time.time() - BOOT_TIME) < CONFIG["settings"]["boot_grace_period"]
    game_id = event["id"]
    status = event["status"]
    state = status["type"]["state"]
    competitions = event.get("competitions", [])
    is_completed = status["type"]["completed"]
    if not competitions:
        return

    comp = competitions[0]
    team_data = comp["competitors"]
    h_team_name = team_data[0]["team"]["displayName"]
    a_team_name = team_data[1]["team"]["displayName"]
    curr_score = f"{team_data[0].get('score','0')}-{team_data[1].get('score','0')}"

    clock_val = status.get("displayClock", status.get("displayValue", "??"))
    clock_str = f"[{clock_val}] " if state == "in" else ""

    if game_id not in MATCH_STATES:

        period = status.get("period", 1)
        is_in_progress = state == "in"
        is_finished = state == "post"
        is_halftime = status["type"].get("description", "").lower() == "halftime"

        boot_stats = {}
        for t in team_data:
            tid = str(t["team"]["id"])
            s_map = {
                s["name"]: int(float(s.get("displayValue", 0)))
                for s in t.get("statistics", [])
            }
            boot_stats[tid] = {
                "corners": s_map.get("wonCorners", 0),
                "sot": s_map.get("shotsOnTarget", 0),
                "shots": s_map.get("totalShots", 0),
            }

        MATCH_STATES[game_id] = {
            "score": curr_score,
            "last_cards_sum": 0,
            "lineup": is_in_progress or is_finished,
            "ko": is_in_progress or is_finished,
            "ht": (is_in_progress and period >= 2) or is_halftime or is_finished,
            "sh": (is_in_progress and period >= 2) or is_finished,
            "ft": is_finished,
            "recap_sent": False,
            "last_seen": time.time(),
            "processed_events": set(),
            "processed_commentary": set(),
            "last_stats": boot_stats,
            "details": None,
            "is_fetching": False,
        }

        if is_startup:
            logging.info(f"Silently initialized {h_team_name} v {a_team_name}")
            return

    gs = MATCH_STATES[game_id]
    gs["last_seen"] = time.time()

    targets = [LEAGUE_CONFIG.get(league, {}).get("webhook", OTHER_WEBHOOK)]
    score_txt = f"**{h_team_name} {curr_score} {a_team_name}**"
    score_changed = curr_score != gs["score"]

    if state == "in":
        for i, team in enumerate(team_data):
            tid = str(team["id"])
            other_team = team_data[1 - i]
            stats_list = {
                s["name"]: int(float(s.get("displayValue", 0)))
                for s in team.get("statistics", [])
            }
            other_stats = {
                s["name"]: int(float(s.get("displayValue", 0)))
                for s in other_team.get("statistics", [])
            }
            prev = gs["last_stats"][tid]

            new_corner = stats_list.get("wonCorners", 0) > prev["corners"]
            new_sot = (
                stats_list.get("shotsOnTarget", 0) > prev["sot"] and not score_changed
            )
            new_shot = (
                stats_list.get("totalShots", 0) > prev["shots"]
                and not score_changed
                and not new_sot
            )

            if any([new_corner, new_sot, new_shot]):
                label = (
                    "Corner"
                    if new_corner
                    else ("Shot on target" if new_sot else "Shot")
                )
                s_key = (
                    "wonCorners"
                    if new_corner
                    else ("shotsOnTarget" if new_sot else "totalShots")
                )

                description = (
                    f"\n\n**Total {label.lower()}s**\n"
                    f"```{team['team']['displayName']:<24.20} {stats_list.get(s_key, 0):>3}\n"
                    f"{other_team['team']['displayName']:<24.20} {other_stats.get(s_key, 0):>3}```\n"
                    f"{score_txt}"
                )

                await enqueue_alert(
                    f"{clock_str} {label}: {team['team']['displayName']}",
                    description,
                    color=int(team["team"].get("color", "3498DB"), 16),
                    url=targets,
                    thumb=team["team"].get("logo"),
                    delete_after=3,
                )

            gs["last_stats"][tid] = {
                "corners": stats_list.get("wonCorners", 0),
                "sot": stats_list.get("shotsOnTarget", 0),
                "shots": stats_list.get("totalShots", 0),
            }

    if score_changed:
        scoring_team = None
        old_h, old_a = map(int, gs["score"].split("-"))
        new_h, new_a = map(int, curr_score.split("-"))

        if new_h > old_h:
            scoring_team = team_data[0]["team"]
        elif new_a > old_a:
            scoring_team = team_data[1]["team"]

        logo = scoring_team.get("logo") if scoring_team else None
        t_name = scoring_team.get("displayName", "GOAL") if scoring_team else "GOAL"

        await enqueue_alert(
            f"{clock_str} GOAL for {t_name}!",
            score_txt,
            url=targets + [GOALS_WEBHOOK],
            thumb=logo,
        )
        gs["score"] = curr_score

    current_cards = sum(
        int(s.get("displayValue", 0))
        for t in team_data
        for s in t.get("statistics", [])
        if s["name"] in ["yellowCards", "redCards"]
    )
    cards_changed = current_cards != gs["last_cards_sum"]

    if (score_changed or cards_changed or gs["details"] is None) and not gs.get(
        "is_fetching"
    ):
        gs["is_fetching"] = True

        async def fetch_wrapper():
            try:
                await update_match_details(
                    league, game_id, event, gs, targets, score_txt, clock_str
                )
            finally:
                gs["is_fetching"] = False

        asyncio.create_task(fetch_wrapper())
        gs["last_cards_sum"] = current_cards

    if gs.get("details") and gs["details"].get("snapshot"):
        current_details = gs["details"]
        scorers_txt = current_details.get("goals_summary", "")

        await handle_match_events(
            gs, current_details, targets, league, score_txt, clock_str, scorers_txt
        )

        await handle_milestones(
            gs,
            current_details,
            event,
            targets,
            score_txt,
            scorers_txt,
            uk_tv,
            league_name,
            curr_score,
            h_team_name,
            a_team_name,
        )

    if is_completed and not gs.get("recap_sent"):
        match_start_ts = (
            datetime.strptime(event["date"], "%Y-%m-%dT%H:%MZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        if (time.time() - match_start_ts) < 12000:
            if gs["details"].get("article"):
                article = gs["details"].get("article")
                await handle_recap(gs, article, targets, score_txt, scorers_txt)


async def monitor():
    logging.info("FootBot Active")
    asyncio.create_task(cleanup_worker())

    fixtures_sent_today = None
    uk_tv = {}
    league_schedules = {}
    last_full_sync = 0
    any_live = False

    for _ in range(DISCORD_WORKERS):
        asyncio.create_task(discord_worker())

    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            today_str = now_dt.strftime("%Y-%m-%d")
            current_timestamp = time.time()

            if not uk_tv:
                logging.info("Initial TV fetch on startup...")
                uk_tv = await get_uk_tv()

            sync_interval = (
                CONFIG["settings"]["sync_interval_live"]
                if any_live
                else CONFIG["settings"]["sync_interval_idle"]
            )
            if current_timestamp - last_full_sync > sync_interval:
                logging.info("Performing full league sync...")
                sync_tasks = [fetch_league_data(l) for l in LEAGUES]
                results = await asyncio.gather(*sync_tasks)
                for l_code, data in results:
                    if data:
                        league_schedules[l_code] = data
                last_full_sync = current_timestamp

            live_leagues = [
                l
                for l, sb in league_schedules.items()
                if any(
                    e["status"]["type"]["state"] == "in" for e in sb.get("events", [])
                )
            ]

            if live_leagues:
                refresh_results = await asyncio.gather(
                    *(fetch_league_data(l) for l in live_leagues)
                )
                for l_code, data in refresh_results:
                    if data:
                        league_schedules[l_code] = data

            any_live = False
            daily_fixtures = {}
            all_match_tasks = []

            for l_code, sb in league_schedules.items():
                l_name = sb.get("leagues", [{}])[0].get("name", l_code.upper())
                events = sb.get("events", [])

                for event in events:
                    m_dt = datetime.strptime(event["date"], "%Y-%m-%dT%H:%MZ").replace(
                        tzinfo=timezone.utc
                    )
                    state = event["status"]["type"]["state"]
                    seconds_until_ko = (m_dt - now_dt).total_seconds()

                    if state == "in" or (3900 >= seconds_until_ko >= -9000):
                        all_match_tasks.append(
                            process_match(l_code, event, l_name, uk_tv)
                        )

                        if state == "in" or (seconds_until_ko <= 600):
                            any_live = True

                    if (
                        0 <= seconds_until_ko <= 86400
                        and fixtures_sent_today != today_str
                    ):
                        if l_name not in daily_fixtures:
                            daily_fixtures[l_name] = []
                        teams = {
                            t["homeAway"]: t["team"]["displayName"]
                            for t in event["competitions"][0]["competitors"]
                        }
                        fixture_entry = f"`{m_dt.strftime('%H:%M')}` {teams.get('home')} vs {teams.get('away')}"
                        if fixture_entry not in daily_fixtures[l_name]:
                            daily_fixtures[l_name].append(fixture_entry)

            if all_match_tasks:
                await asyncio.gather(*all_match_tasks)

            if (
                fixtures_sent_today != today_str
                and now_dt.hour == CONFIG["settings"]["daily_fixtures_hour"]
            ):
                uk_tv = await get_uk_tv()
                if daily_fixtures:
                    fixtures_msg = ""
                    for n, m in daily_fixtures.items():
                        fixtures_msg += f"**{n}**\n" + "\n".join(m) + "\n\n"
                    await enqueue_alert(
                        "📅 Today's Fixtures",
                        fixtures_msg,
                        url=[FIXTURES_WEBHOOK],
                        delete_after=24,
                    )
                fixtures_sent_today = today_str
                REPLAY_TRACKER.clear()

            for game_id in list(MATCH_STATES.keys()):
                gs = MATCH_STATES.get(game_id)
                if isinstance(gs, dict):
                    last_seen = gs.get("last_seen", 0)
                    if gs.get("ft") and (
                        current_timestamp - last_seen
                        > CONFIG["settings"]["match_expiry_seconds"]
                    ):
                        del MATCH_STATES[game_id]

            wait_time = (LIVE_INTERVAL if any_live else IDLE_INTERVAL) + random.uniform(
                0.5, 2.0
            )
            await asyncio.sleep(wait_time)

        except Exception as e:
            logging.exception(f"Monitor Loop Error: {e}")
            await asyncio.sleep(CONFIG["settings"]["loop_error_delay"])


if __name__ == "__main__":
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(async_client.aclose())
        loop.close()
