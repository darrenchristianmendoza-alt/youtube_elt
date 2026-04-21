import os
import logging
import requests
from typing import Optional
from dotenv import load_dotenv
from dataclasses import dataclass
import json
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"


# ── Data containers ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ChannelConfig:
    """Immutable configuration for a YouTube channel lookup."""
    api_key: str
    channel_id: str


# ── Custom exceptions ──────────────────────────────────────────────────────────

class YouTubeAPIError(Exception):
    """Raised when the YouTube API returns an unexpected response."""


class PlaylistNotFoundError(YouTubeAPIError):
    """Raised when the uploads playlist cannot be found for a channel."""


# ── API client ─────────────────────────────────────────────────────────────────

class YouTubeClient:
    """Thin HTTP wrapper around the YouTube Data API v3."""

    def __init__(self, api_key: str, base_url: str = YOUTUBE_API_BASE_URL, timeout: int = 10):
        if not api_key:
            raise ValueError("api_key must not be empty.")
        self._api_key = api_key          # kept private; never exposed in repr
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()

    def get(self, endpoint: str, params: dict) -> dict:
        """
        Perform a GET request against *endpoint* (e.g. '/channels').

        The API key is injected here so callers never have to touch it.
        """
        params = {**params, "key": self._api_key}   # copy; don't mutate caller's dict
        url = f"{self._base_url}/{endpoint.lstrip('/')}"

        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as exc:
            raise YouTubeAPIError(f"HTTP error from YouTube API: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise YouTubeAPIError(f"Network error: {exc}") from exc

    def close(self) -> None:
        self._session.close()

    # Context-manager support
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

# ── Domain logic ───────────────────────────────────────────────────────────────

class UploadPlaylist:
    """Retrieves the uploads playlist ID for a YouTube channel."""

    def __init__(self, client: YouTubeClient, channel_id: str):
        if not channel_id:
            raise ValueError("channel_id must not be empty.")
        self._client = client
        self._channel_id = channel_id

    def get_playlist_id(self) -> Optional[str]:
        """
        Return the uploads playlist ID for the configured channel,
        or raise PlaylistNotFoundError if it cannot be found.
        """
        logger.info("Fetching uploads playlist for channel %s", self._channel_id)

        data = self._client.get(
            "channels",
            params={"part": "contentDetails", "id": self._channel_id},
        )

        items = data.get("items")
        if not items:
            raise PlaylistNotFoundError(
                f"No channel found with id '{self._channel_id}'."
            )

        try:
            playlist_id: str = (
                items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
            )
        except (KeyError, IndexError) as exc:
            raise PlaylistNotFoundError(
                "Uploads playlist key missing from API response."
            ) from exc

        logger.info("Found uploads playlist: %s", playlist_id)
        return playlist_id

class VideoPlayList:
    """Retrieves video playlist IDs for a YouTube channel."""

    def __init__(self, client: YouTubeClient, channel_id: str):
        if not channel_id:
            raise ValueError("channel_id must not be empty.")
        self._client = client
        self._channel_id = channel_id

    def get_videolist_id(self, playlist_id: str) -> list[dict]:
        """
        Return ALL video items from the given playlist ID by paginating
        through every available page using nextPageToken.
        """
        logger.info("Fetching all videos from playlist %s", playlist_id)

        all_items = []
        page_token: Optional[str] = None
        page_num = 1

        while True:
            params = {
                "part": "snippet",
                "playlistId": playlist_id,
                "maxResults": 50,
            }

            # Only add pageToken on subsequent pages
            if page_token:
                params["pageToken"] = page_token

            data = self._client.get("playlistItems", params=params)
            items = data.get("items", [])
            all_items.extend(items)

            logger.info("Page %d: retrieved %d videos (total so far: %d)",
                        page_num, len(items), len(all_items))

            # If there's no nextPageToken, we've reached the last page
            page_token = data.get("nextPageToken")
            if not page_token:
                break

            page_num += 1

        if not all_items:
            raise PlaylistNotFoundError(
                f"No videos found in playlist '{playlist_id}'."
            )

        logger.info("Finished. Total videos fetched: %d", len(all_items))
        return all_items

class VideoDetails:
    """Fetches full video metadata for a list of video IDs."""

    def __init__(self, client: YouTubeClient):
        self._client = client

    def get_video_data(self, video_ids: list[str]) -> list[dict]:
        """
        Return full video details for all given video IDs.
        Batches requests in chunks of 50 (YouTube API limit).
        """
        all_videos = []

        # Split into chunks of 50
        chunks = [video_ids[i:i + 50] for i in range(0, len(video_ids), 50)]

        for chunk_num, chunk in enumerate(chunks, start=1):
            logger.info("Fetching video details batch %d/%d (%d videos)",
                        chunk_num, len(chunks), len(chunk))

            data = self._client.get(
                "videos",
                params={
                    "part": "snippet,statistics,contentDetails",
                    "id": ",".join(chunk),
                },
            )

            items = data.get("items", [])
            all_videos.extend(items)
            logger.info("Batch %d: retrieved %d video records", chunk_num, len(items))

        logger.info("Finished. Total video details fetched: %d", len(all_videos))
        return all_videos


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    # Load credentials from environment variables — never hard-code secrets.
    api_key = os.environ.get("YOUTUBE_API_KEY")
    channel_id = os.environ.get("YOUTUBE_CHANNEL_ID", "UCX6OQ3DkcsbYNE6H8uQQuVA")

    if not api_key:
        raise EnvironmentError(
            "YOUTUBE_API_KEY environment variable is not set. "
            "Add it to your .env file or export it in your shell."
        )

    with YouTubeClient(api_key=api_key) as client:
        playlist = UploadPlaylist(client, channel_id)
        playlist_id = playlist.get_playlist_id()

        video = VideoPlayList(client, channel_id)
        video_ids = video.get_videolist_id(playlist_id)
        video_ids = [
            vi["snippet"]["resourceId"]["videoId"]
            for vi in video_ids
        ]

        details = VideoDetails(client)
        video_data = details.get_video_data(video_ids)

        slim_data = [
            {
                "id":         video["id"],
                "title":      video["snippet"]["title"],
                "published":  video["snippet"]["publishedAt"],
                "duration":   video.get("contentDetails", {}).get("duration", "N/A"),
                "views":      video.get("statistics", {}).get("viewCount", "0"),
                "likes":      video.get("statistics", {}).get("likeCount", "0"),
                "comments":   video.get("statistics", {}).get("commentCount", "0"),
            }
            for video in video_data
        ]

        filename = "data-" + datetime.now().strftime("%Y-%m-%d") + ".json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(slim_data, f, indent=2, ensure_ascii=False)
        logger.info("Saved %d videos to %s", len(slim_data), filename)
        print(f"Data saved to: {filename}")

if __name__ == "__main__":
    main()