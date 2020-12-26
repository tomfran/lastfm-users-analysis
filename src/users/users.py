import requests

from config import *

# exec(open("config.py").read())

def get_user_most_recent_track(user):
    response = requests.get(BASE + "method=user.getrecenttracks&user=" +
                            user + "&api_key=" + KEY + "&format=json" + "&limit=500")
    try:
        return response.json()['recenttracks']['track']
    except:
        return None

def get_user_friends(user):
    response = requests.get(BASE + "method=user.getfriends&user=" +
                            user + "&api_key=" + KEY + "&format=json" + "&limit=500")
    try:
        return response.json()['friends']['user']
    except:
        return []

def get_user_info(user):
    response = requests.get(BASE + "method=user.getinfo&user=" +
                            user + "&api_key=" + KEY + "&format=json")
    return response.json()['user']
