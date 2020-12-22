from users import *
import json

exec(open("config.py").read())

if __name__ == "__main__":
    with open(LAST_USER_FILE) as f:
        names = f.readlines()
    
    users = []

    for name in names:
        name = name.strip()
        info = get_user_info(name)
        user = {'name': name, 
                'playcount' : info['playcount'],
                'gender': info['gender'],
                'subscriber': info['subscriber'],
                'url': info['url'],
                'country': info['country'],
                'age': info['age']}
        users.append(user)
    
    with open(USER_JSON, 'w') as f:
        json.dump(users, f)
