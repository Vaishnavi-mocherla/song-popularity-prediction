import os
import requests
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables at the start
load_dotenv()


def get_access_token(client_id, client_secret):
    """Fetches the Spotify access token using the client credentials flow."""
    token_url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "client_credentials"
    }
    response = requests.post(token_url, data=payload,
                             auth=(client_id, client_secret))
    return response.json().get("access_token")


def get_artist_genres(artist_ids, access_token, base_url):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    genres = set()
    for artist_id in artist_ids:
        url = f"{base_url}/artists/{artist_id}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(
                f"Error fetching artist data: {response.status_code} - {response.text}")
            continue  # Skip to the next artist if there's an error
        artist_data = response.json()
        genres.update(artist_data.get('genres', []))
    return list(genres)


def search_tracks(year, month, access_token):
    base_url = os.getenv("BASE_URL")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    url = f"{base_url}/search"
    offset = 0
    limit = 50
    all_tracks = []

    while True:
        params = {
            "q": f"year:{year} month:{month}",
            "type": "track",
            "limit": limit,
            "offset": offset
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(
                f"Error fetching tracks: {response.status_code} - {response.text}")
            break  # Stop fetching if there's a critical API error
        tracks = response.json().get('tracks', {}).get('items', [])
        if not tracks:
            break

        for track in tqdm(tracks):
            artist_ids = [artist['id'] for artist in track['artists']]
            genres = get_artist_genres(artist_ids, access_token, base_url)
            track_details = {
                'id': track['id'],
                'popularity': track['popularity'],
                'artists': ', '.join(artist['name'] for artist in track['artists']),
                'genres': genres,
                'year': year,
                'month': month
            }
            all_tracks.append(track_details)

        offset += limit

    return all_tracks


def get_track_features(track_details, access_token, base_url):
    """Fetch audio features for a specific track and update with additional details."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    url = f"{base_url}/audio-features/{track_details['id']}"
    response = requests.get(url, headers=headers)
    features = response.json()
    features.update({
        'popularity': track_details['popularity'],
        'artists': track_details['artists'],
        'year': track_details['year'],
        'month': track_details['month']
    })
    return features


def main():
    """Main function to orchestrate data retrieval and storage."""
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    token = get_access_token(client_id, client_secret)

    features_list = []
    for year in range(2022, 2025):
        for month in range(1, 13):
            print(f"Getting tracks for {month}/{year}")
            tracks = search_tracks(year, month, token)
            for track in tqdm(tracks):
                features = get_track_features(
                    track, token, os.getenv("BASE_URL"))
                features_list.append(features)

    # for month in range(1, 5):
    #     print(f"Getting tracks for {month}/{2024}")
    #     tracks = search_tracks(2024, month, token)
    #     for track in tqdm(tracks):
    #         features = get_track_features(
    #             track, token, os.getenv("BASE_URL"))
    #         features_list.append(features)

    df = pd.DataFrame(features_list)
    df.to_csv("tracks_by_month.csv", index=False)


if __name__ == "__main__":
    main()
