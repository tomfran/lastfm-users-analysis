import json
import datetime as dt
from users import *

exec(open("config.py").read())

def is_active_user(user):
    ret = False

    tracks = get_user_most_recent_track(user)
    if tracks and len(tracks) > 0:
        try:
            date = int(tracks[0]['date']['uts'])
            ret = (dt.datetime.now() - dt.datetime.fromtimestamp(date)).days < (7 * WEEKS_TO_BE_ACTIVE)
        except:
            pass

    return ret

def is_valid_user(user, check_countries):
    ok_countries = True if (not check_countries) or (
        check_countries and user['country'] in COUNTRIES) else False
    return (ok_countries and is_active_user(user['name']))
    
def get_users(check_countries=False):

    filename = 'users' + str(dt.datetime.now().timestamp()) + '.json'

    valid_users = [SEED_USER]
    users = [SEED_USER]
    current_user = 0

    while len(valid_users) < N_USER_TO_TRACK:
        friends = get_user_friends(users[current_user])
        for friend in friends:
            users.append(friend['name'])
            if (friend['name'] not in valid_users) and is_valid_user(friend, check_countries):
                valid_users.append(friend['name'])

                print('VALID ', friend['name'])
                with open(filename, "a+") as f:
                    f.write("{}\n".format(friend['name']))

        current_user += 1
        if current_user >= len(users):
            break

    return valid_users

if __name__ == "__main__":
    get_users(check_countries=True)
