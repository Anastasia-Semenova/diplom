import vk_api
from datetime import datetime, timedelta
import pandas as pd

data_file = 'tatarstan/5_data.csv'
filtered_file = 'tatarstan/5_filtered.csv'


def get_info(vk, id):
    sender_info = vk.users.get(user_ids={id},
                               fields='city, country, home_town, bdate, counters, followers_count, timezone, status, sex, followers',
                               version=5.89)[0]
    if not sender_info['is_closed']:
        int_id = sender_info['id']
        print(sender_info)
        count = 100
        now = datetime.now()
        year_ago = now - timedelta(days=365)
        # параметры запроса
        params = {
            'owner_id': int_id,
            'count': count
        }
        paramsphoto = {
            'owner_id': int_id,
            'count': count,
            'extended': 1,
            'fields': 'photo'
        }
        responsephotos = vk.photos.getAll(**paramsphoto)
        response = vk.wall.get(**params)
        posts_count = 0
        posts_likes = 0
        for item in response['items']:
            date = datetime.fromtimestamp(item['date'])
            if date >= year_ago:
                posts_count += 1
                posts_likes += item['likes']['count']
        photo_likes = 0
        photos_count = 0
        for item in responsephotos['items']:
            date = datetime.fromtimestamp(item['date'])
            if date >= year_ago:
                photos_count += 1
                photo_likes += item['likes']['count']

        id = sender_info['id']
        try:
            city = sender_info['city']['id']
        except KeyError:
            city = -1
        try:
            country = sender_info['country']['id']
        except KeyError:
            country = -1
        # home_town = sender_info['home_town']
        friend_count = sender_info['counters']['friends']
        followers_count = sender_info['counters']['followers']
        try:
            bdate = sender_info['bdate']
        except KeyError:
            bdate = -1
        try:
            groups_count = sender_info['counters']['groups']
        except KeyError:
            groups_count = -1
        sex = sender_info['sex']
        pages = sender_info['counters']['pages']
        if bdate != -1:
            birthdate_date = datetime.strptime(bdate, "%d.%m.%Y").date()
            today_date = datetime.today().date()
            age = today_date.year - birthdate_date.year - (
                    (today_date.month, today_date.day) < (birthdate_date.month, birthdate_date.day))
        else:
            age = -1
        data = [
            id, sex, age, city, country, friend_count, followers_count, pages, groups_count, posts_count, posts_likes,
            photos_count,
            photo_likes
        ]
        write_csv(data)


def write_csv(data):
    # wr = pd.DataFrame(columns=['id', 'sex', 'age', 'city', 'country', 'friend_count', 'followers_count', 'pages', 'groups', 'posts_last_year', 'posts_likes', 'photos_last_year', 'photo_likes'])
    # wr.to_csv('1_data.csv', index=False)

    df = pd.read_csv(data_file)
    df.loc[len(df)] = data
    df.set_index('id', inplace=True)
    df = df[~df.index.duplicated(keep='last')]
    df.to_csv(data_file, index=True)


def connect_vk():
    vk_session = vk_api.VkApi(
        token='vk1.a.DVI-JBMlYVHkF6_oYUF50bONQvyYXQvHMFS7lez4bI-7NOoYKF4Sf_BYD6zPQYLTi8rXZn6fNMLvV_57Gsfc2XGeUskHNDJObo3qPMFRMZcdq6Lg1GLfmDWrY_N3c9iuYKk3FIfpB65HX8bur23T_4_TUhC4QeugQSB9WFVV3tA_CWELvjYg1pE3qfrN9x0frz7WJ37Tye_qnQuUIU5Oeg',
        login='+79991621338', password='Glotai)900')
    vk = vk_session.get_api()
    # get_info(vk, id)
    return vk


def collect_data(vk):
    collecting = vk.users.search(count=1000, city=60, sex=2, age_from=18, age_to=24, sort=1)
    return collecting


def clean_data(id_country):
    df = pd.read_csv(data_file)
    # Выбор строк, где значение столбца city равно 60
    df = df[df['groups'] != -1]
    df = df[df['city'] == 60]
    df.to_csv(filtered_file, index=False)


if __name__ == "__main__":
    vk = connect_vk()
    coll = collect_data(vk)
    print(coll)
    for item in coll['items']:
        get_info(vk, item['id'])
    clean_data(60)
