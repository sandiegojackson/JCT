"""
YouTube Performance Monitor — Monday Morning Cron Job
Deploys on Render as a cron job (runs every Monday at 8:00 AM PT)
Pulls YouTube Analytics for all published videos and logs snapshots to Notion.

Environment variables required in Render:
  YOUTUBE_CLIENT_ID
  YOUTUBE_CLIENT_SECRET
  YOUTUBE_REFRESH_TOKEN
  YOUTUBE_CHANNEL_ID
  NOTION_API_KEY
  NOTION_DATABASE_ID
"""

import os
import requests
from datetime import date, datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────

YOUTUBE_CLIENT_ID     = os.environ["YOUTUBE_CLIENT_ID"]
YOUTUBE_CLIENT_SECRET = os.environ["YOUTUBE_CLIENT_SECRET"]
YOUTUBE_REFRESH_TOKEN = os.environ["YOUTUBE_REFRESH_TOKEN"]
YOUTUBE_CHANNEL_ID    = os.environ["YOUTUBE_CHANNEL_ID"]
NOTION_API_KEY        = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID    = os.environ["NOTION_DATABASE_ID"]

NOTION_VERSION = "2022-06-28"

# Tracking intervals in days from publish date
INTERVALS = {
    "Day 7":  7,
    "Day 14": 14,
    "Day 32": 32,
    "Day 60": 60,
    "Day 88": 88,
}

# ── YouTube Auth ──────────────────────────────────────────────────────────────

def get_access_token():
    """Exchange refresh token for a fresh access token."""
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     YOUTUBE_CLIENT_ID,
        "client_secret": YOUTUBE_CLIENT_SECRET,
        "refresh_token": YOUTUBE_REFRESH_TOKEN,
        "grant_type":    "refresh_token",
    })
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("✅ Access token obtained")
    return token


# ── YouTube Data ──────────────────────────────────────────────────────────────

def get_published_videos(access_token):
    """
    Fetch all published long-form videos from the channel.
    Returns list of dicts: {video_id, title, publish_date}
    """
    videos = []
    page_token = None
    headers = {"Authorization": f"Bearer {access_token}"}

    while True:
        params = {
            "part":       "snippet",
            "channelId":  YOUTUBE_CHANNEL_ID,
            "maxResults": 50,
            "order":      "date",
            "type":       "video",
        }
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            headers=headers,
            params=params
        )
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            snippet = item["snippet"]
            publish_date = datetime.fromisoformat(
                snippet["publishedAt"].replace("Z", "+00:00")
            ).date()
            videos.append({
                "video_id":     item["id"]["videoId"],
                "title":        snippet["title"],
                "publish_date": publish_date,
            })

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    print(f"✅ Found {len(videos)} published videos")
    return videos


def get_video_analytics(access_token, video_id, publish_date, end_date):
    """
    Pull cumulative metrics for a video from publish date to end_date.
    Returns dict: {views, watch_time_minutes, impressions, ctr}
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    # Views and Watch Time from YouTube Analytics API
    analytics_resp = requests.get(
        "https://youtubeanalytics.googleapis.com/v2/reports",
        headers=headers,
        params={
            "ids":        f"channel=={YOUTUBE_CHANNEL_ID}",
            "startDate":  publish_date.isoformat(),
            "endDate":    end_date.isoformat(),
            "metrics":    "views,estimatedMinutesWatched",
            "filters":    f"video=={video_id}",
            "dimensions": "video",
        }
    )
    analytics_resp.raise_for_status()
    analytics_data = analytics_resp.json()

    views = 0
    watch_time = 0
    rows = analytics_data.get("rows", [])
    if rows:
        views      = int(rows[0][1])
        watch_time = int(rows[0][2])

    # Impressions and CTR — not always available at the individual video level;
    # wrap in try/except so views/watch-time snapshots still get written on failure.
    impressions = 0
    ctr = 0.0
    try:
        impressions_resp = requests.get(
            "https://youtubeanalytics.googleapis.com/v2/reports",
            headers=headers,
            params={
                "ids":        f"channel=={YOUTUBE_CHANNEL_ID}",
                "startDate":  publish_date.isoformat(),
                "endDate":    end_date.isoformat(),
                "metrics":    "impressions,impressionsClickThroughRate",
                "filters":    f"video=={video_id}",
            }
        )
        impressions_resp.raise_for_status()
        imp_rows = impressions_resp.json().get("rows", [])
        if imp_rows:
            impressions = int(imp_rows[0][0])
            ctr         = round(float(imp_rows[0][1]) * 100, 2)  # convert to %
    except Exception as e:
        body = ""
        try:
            body = impressions_resp.json()
        except Exception:
            pass
        print(f"  ⚠️  Impressions unavailable for {video_id}: {e} | {body}")

    return {
        "views":               views,
        "watch_time_minutes":  watch_time,
        "impressions":         impressions,
        "ctr":                 ctr,
    }


# ── Notion ────────────────────────────────────────────────────────────────────

def snapshot_exists(video_id, interval_label, log_date):
    """Check if a snapshot already exists in Notion to avoid duplicates."""
    headers = {
        "Authorization":  f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type":   "application/json",
    }
    payload = {
        "filter": {
            "and": [
                {"property": "YouTube Video ID", "rich_text": {"equals": video_id}},
                {"property": "Interval",         "select":    {"equals": interval_label}},
                {"property": "Log Date",         "date":      {"equals": log_date.isoformat()}},
            ]
        }
    }
    resp = requests.post(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
        headers=headers,
        json=payload
    )
    resp.raise_for_status()
    return len(resp.json().get("results", [])) > 0


def write_snapshot_to_notion(video, interval_label, log_date, metrics):
    """Write a single performance snapshot row to the Notion database."""
    headers = {
        "Authorization":  f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type":   "application/json",
    }

    snapshot_name = f"{video['title']} — {interval_label}"

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": {
            "Snapshot Name": {
                "title": [{"text": {"content": snapshot_name}}]
            },
            "Video Title": {
                "rich_text": [{"text": {"content": video["title"]}}]
            },
            "YouTube Video ID": {
                "rich_text": [{"text": {"content": video["video_id"]}}]
            },
            "Publish Date": {
                "date": {"start": video["publish_date"].isoformat()}
            },
            "Log Date": {
                "date": {"start": log_date.isoformat()}
            },
            "Interval": {
                "select": {"name": interval_label}
            },
            "Type": {
                "select": {"name": "Video"}
            },
            "Views": {
                "number": metrics["views"]
            },
            "Watch Time (min)": {
                "number": metrics["watch_time_minutes"]
            },
            "Impressions": {
                "number": metrics["impressions"]
            },
            "CTR (%)": {
                "number": metrics["ctr"]
            },
        }
    }

    resp = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers,
        json=payload
    )
    resp.raise_for_status()
    print(f"  ✅ Logged: {snapshot_name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n🎬 YouTube Performance Monitor — {date.today().isoformat()}\n")

    access_token = get_access_token()
    videos       = get_published_videos(access_token)
    today        = date.today()

    snapshots_written = 0
    snapshots_skipped = 0

    for video in videos:
        publish_date = video["publish_date"]
        days_live    = (today - publish_date).days

        print(f"\n📹 {video['title']} (published {publish_date}, {days_live} days live)")

        for interval_label, interval_days in INTERVALS.items():
            # Only log this interval if the video has been live long enough
            if days_live < interval_days:
                print(f"  ⏭️  Skipping {interval_label} — not reached yet")
                continue

            # The snapshot date is publish_date + interval_days
            # but we log it on the Monday it's captured (today)
            snapshot_end_date = publish_date + timedelta(days=interval_days)

            # Skip if we already logged this interval for this video
            if snapshot_exists(video["video_id"], interval_label, today):
                print(f"  ⏭️  Skipping {interval_label} — already logged today")
                snapshots_skipped += 1
                continue

            try:
                metrics = get_video_analytics(
                    access_token,
                    video["video_id"],
                    publish_date,
                    snapshot_end_date,
                )
                write_snapshot_to_notion(video, interval_label, today, metrics)
                snapshots_written += 1
            except Exception as e:
                print(f"  ❌ Error on {interval_label}: {e}")

    print(f"\n✅ Done. {snapshots_written} snapshots written, {snapshots_skipped} skipped.\n")


if __name__ == "__main__":
    main()
