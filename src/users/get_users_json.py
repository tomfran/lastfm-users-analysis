# exec(open("config.py").read())
from users import *
import json

# from .config import *

if __name__ == "__main__":
    with open(LAST_USER_FILE) as f:
        names = f.readlines()
    
    users = []

    for i, name in enumerate(names):
        print(i)
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
        f.write(json.dumps(users, indent=4, sort_keys=True, ensure_ascii=False))
