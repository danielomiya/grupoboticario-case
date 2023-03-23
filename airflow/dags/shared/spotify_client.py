import time
import typing as t

import requests


def pluralize(name: str) -> str:
    """
    Pluralizes a string by adding an "s" at the end of it

    :param name: the name to be pluralized
    :return: a plural form of the given name
    """
    # silly method to make code more readable
    return name + "s"


class SpotifyAuth(requests.auth.AuthBase):
    """
    Auth implementation to make requests to the Spotify API
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        session: requests.Session = None,
    ) -> None:
        """
        Initialize the class instances

        :param client_id: the Spotify API client_id
        :param client_secret: the Spotify API client_secret
        :param session: optional requests.Session, defaults to None
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: str = None
        self.expires_at: int = None

        if session:
            self.session = session
        else:
            self.session = requests.Session()

    def __call__(self, r: requests.Request) -> requests.Request:
        """
        Adds the Authorization header with a token from the current session

        :param r: request intercepted before sent
        :return: modified request with auth info
        """
        if not r.url.startswith("https://api.spotify.com"):
            return r

        if self.is_current_token_expired:
            token_data = self.request_access_token()
            self.store_token(token_data)

        r.headers["Authorization"] = f"Bearer {self.access_token}"
        return r

    def request_access_token(self) -> t.Dict[str, t.Any]:
        """
        Requests access to send subsequent requests for protected resources

        :return: access details returned as JSON response in dict format
        """
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
        """
        Stores the access token and its expiration time

        :param token_data: dict of the API response
        """
        self.expires_at = int(time.time()) + token_data["expires_in"]
        self.access_token = token_data["access_token"]

    @property
    def is_current_token_expired(self) -> bool:
        """
        Checks if the token is already expired or not

        :return: whether token has been expired
        """
        if self.expires_at is None:
            return True

        now = int(time.time())
        return now >= self.expires_at


class SpotifyClient:
    """
    Client for the Spotify's API, allowing to abstract the access to it
    """

    def __init__(self, client_id: str, client_secret: str) -> None:
        """
        Initialize the class instances

        :param client_id: the Spotify API client_id
        :param client_secret: the Spotify API client_secret
        """
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
    ) -> t.Union[
        t.List[t.Dict[str, t.Any]],
        t.Dict[str, t.List[t.Dict[str, t.Any]]],
    ]:
        """
        Gets the result of the search query

        :param query: search query
        :param type: type of query like album, artist, playlist or tract, etc
        :param market: the market on which the song was released, defaults to "BR"
        :param limit: the number of objects to return in a single search,
                      defaults to 50
        :param offset: the position of objects at which the search will start,
                       defaults to 0
        :return: either list of dict or a dict having data about various
                 elements based on the type of query
        """
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
