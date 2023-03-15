import time
import typing as t

import requests


def pluralize(name: str) -> str:
    # silly method to make code more readable
    return name + "s"


class SpotifyAuth(requests.auth.AuthBase):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        session: requests.Session = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: str = None
        self.expires_at: int = None

        if session:
            self.session = session
        else:
            self.session = requests.Session()

    def __call__(self, r: requests.Request) -> requests.Request:
        if not r.url.startswith("https://api.spotify.com"):
            return r

        if self.is_current_token_expired:
            token_data = self.request_access_token()
            self.store_token(token_data)

        r.headers["Authorization"] = f"Bearer {self.access_token}"
        return r

    def request_access_token(self):
        response = self.session.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        response.raise_for_status()
        return response.json()

    def store_token(self, token_data: t.Dict[str, t.Any]) -> None:
        self.expires_at = int(time.time()) + token_data["expires_in"]
        self.access_token = token_data["access_token"]

    @property
    def is_current_token_expired(self) -> bool:
        if self.expires_at is None:
            return True

        now = int(time.time())
        return now >= self.expires_at


class SpotifyClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.session = requests.Session()
        self.session.auth = SpotifyAuth(
            client_id, client_secret, session=self.session
        )

    def search(
        self,
        query: str,
        type: str,
        market: str = "BR",
        limit: int = 50,
        offset: int = 0,
    ) -> t.Dict[str, t.Any]:
        response = self.session.get(
            "https://api.spotify.com/v1/search",
            headers={
                "Accept": "application/json",
            },
            params={
                "q": query,
                "market": market,
                "type": type,
                "limit": limit,
                "offset": offset,
            },
        )
        response.raise_for_status()
        json = response.json()

        if "," not in type:
            return json[pluralize(type)]["items"]
        else:
            types = type.split(",")
            return {t: json[pluralize(t)]["items"] for t in types}

    def __enter__(self) -> "SpotifyClient":
        return self

    def __exit__(self, *args) -> None:
        self.session.close()
