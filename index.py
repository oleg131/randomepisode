from flask import Flask, redirect, render_template, jsonify, request, url_for
from sqlalchemy import create_engine
import pandas as pd

import requests
from unidecode import unidecode
import re, datetime

import db

app = Flask(__name__)

engine = create_engine('mysql+mysqldb://{}:{}@{}/{}'.format(db.user, db.pwd, db.host, db.db), pool_size=2, pool_recycle=3600)

def now():
    return re.sub('\D', '', str(datetime.datetime.now()).split('.', 1)[0])

def get_show_id_by_title(title):

    statement = '''
    SELECT * FROM shows
    WHERE title = '{}'
    '''.format(title)
    with engine.connect() as c:
        conn = c.connection
        df = pd.read_sql(statement, conn)

    if len(df)!=0:
        return df['show_id'].iloc[0]
    else:
        return None

def get_user_id_by_token(user):

    statement = '''
    SELECT * FROM users
    WHERE token = '{}'
    '''.format(user)
    with engine.connect() as c:
        conn = c.connection
        df = pd.read_sql(statement, conn)

    if len(df)!=0:
        return df['user_id'].iloc[0]
    else:
        return None

@app.route('/contact', methods=['GET', 'POST'])
def contact():
    if request.method == 'POST':
        alltext = request.form['name'] + '\n' + request.form['email'] + '\n' + request.form['type'] + '\n' + request.form['text']
        with open('contact/{}.txt'.format(now()), "w") as text_file:
            text_file.write(alltext)
        return render_template('contact.html', sent=True)
    else:
        return render_template('contact.html')


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        user = request.form['user']
        return redirect('/{}'.format(user), code=302)
    else:
        return render_template('index.html')
    # return '<a href=\'/new/\'>Generate new user</a>'

@app.route('/new/')
def new_user():
    import random, string
    rnd = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(50))
    return redirect('/{}/edit'.format(rnd), code=302)

@app.route('/<user>/delete/<show_id>')
def delete(user, show_id):

    user_id = get_user_id_by_token(user)

    with engine.connect() as c:
        conn = c
        trans = conn.begin()
        statement = '''DELETE FROM users_shows WHERE user_id='{}' AND show_id='{}';'''.format(user_id, show_id)
        result = conn.execute(statement)
        statement = '''DELETE FROM episodes WHERE user_id='{}' AND show_id='{}';'''.format(user_id, show_id)
        result = conn.execute(statement)
        trans.commit()

    return redirect('/{}/edit'.format(user), code=302)


@app.route('/<user>/edit', methods=['GET', 'POST'])
def edit(user):

    extra = ''

    if request.method == 'POST':
        show = request.form['show']

        r = requests.get('http://api.tvmaze.com/singlesearch/shows?q={}'.format(show))
        if r:
            title = r.json()['name']
            title = unidecode(title)
            title = re.sub('[^a-zA-Z0-9-_*. ]', '', title)

            statement = '''
            SELECT * FROM users
            JOIN users_shows using(user_id)
            JOIN shows using(show_id)
            WHERE token = '{}'
            AND title = '{}'
            '''.format(user, title)
            with engine.connect() as c:
                conn = c.connection
                user_shows = pd.read_sql(statement, conn)

            if len(user_shows)==0:

                user_id = get_user_id_by_token(user)
                if not user_id:
                    df = pd.DataFrame({'token': user}, index=[0])
                    with engine.connect() as c:
                        conn = c.connection
                        df.to_sql('users', engine, if_exists='append', index=False)
                    user_id = get_user_id_by_token(user)

                show_id = get_show_id_by_title(title)
                if not show_id:
                    df = pd.DataFrame({'title': title}, index=[0])
                    with engine.connect() as c:
                        conn = c.connection
                        df.to_sql('shows', engine, if_exists='append', index=False)
                    show_id = get_show_id_by_title(title)

                df = pd.DataFrame({'user_id': user_id, 'show_id': show_id}, index=[0])
                with engine.connect() as c:
                    conn = c.connection
                    df.to_sql('users_shows', engine, if_exists='append', index=False)

        else:
            extra = 'no_such_show'

        # return redirect('/{}/edit'.format(user), code=302)

    # else:

    statement = '''
    SELECT * FROM users
    JOIN users_shows using(user_id)
    JOIN shows using(show_id)
    WHERE token = '{}'
    '''.format(user)
    with engine.connect() as c:
        conn = c.connection
        df = pd.read_sql(statement, conn)

    return render_template('edit.html', show_ids=df['show_id'].tolist(), shows=df['title'].tolist(), user=user, extra=extra)


@app.route('/<user>')
def userpage(user, methods=['GET', 'POST']):

    statement = '''
    SELECT * FROM users
    JOIN users_shows using(user_id)
    JOIN shows using(show_id)
    WHERE token = '{}'
    '''.format(user)
    with engine.connect() as c:
        conn = c.connection
        df = pd.read_sql(statement, conn)
    user_shows = df.drop_duplicates()

    statement = '''
    SELECT * FROM episodes
    JOIN shows using(show_id)
    JOIN users using(user_id)
    WHERE users.token = '{}'
    '''.format(user)
    with engine.connect() as c:
        conn = c.connection
        user_episodes = pd.read_sql(statement, conn)

    user_episodes['remote_id'] = 0

    nonwatched = []
    for i, show in user_shows.iterrows():

        title = show['title']
        show_id = show['show_id']

        r = requests.get('http://api.tvmaze.com/singlesearch/shows?q={}&embed=episodes'.format(title))

        all_episodes = pd.DataFrame([[i['season'], i['number'], show_id, title] for i in r.json()['_embedded']['episodes']])
        all_episodes.columns = ['season', 'episode', 'show_id', 'title']

        watched_episodes = user_episodes[user_episodes['title']==title]

        merged = pd.concat([all_episodes, watched_episodes[all_episodes.columns]])
        merged.drop_duplicates(keep=False, inplace=True)

        merged['remote_id'] = r.json()['id']

        nonwatched.append(merged)

    if len(nonwatched)==0:
        return render_template('watch.html', title='none', user=user)

    nonwatched = pd.concat(nonwatched)

    if len(nonwatched)<1:
        return render_template('watch.html', title='reset', user=user)

    random = nonwatched.sample(1)

    random['user_id'] = get_user_id_by_token(user)

    with engine.connect() as c:
        conn = c.connection
        random[['user_id', 'show_id', 'season', 'episode']].to_sql('episodes', engine, if_exists='append', index=False)

    title = random['title'].iloc[0]
    season = str(int(random['season'].iloc[0]))
    episode = str(int(random['episode'].iloc[0]))
    rid = random['remote_id'].iloc[0]

    r = requests.get('http://api.tvmaze.com/shows/{}/episodebynumber?season={}&number={}'.format(rid, season, episode))
    name = r.json()['name']
    summary = str(r.json()['summary']).replace('<p>', '').replace('</p>', '')
    image = r.json()['image']
    if image is not None:
        image = image['original']


    return render_template('watch.html', title=title, season=season, episode=episode,
        name=name, summary=summary, image=image, user=user)


@app.route('/<user>/reset')
def reset(user):
    user_id = get_user_id_by_token(user)

    with engine.connect() as c:
        conn = c
        trans = conn.begin()
        statement = '''DELETE FROM episodes WHERE user_id='{}';'''.format(user_id)
        result = conn.execute(statement)
        trans.commit()

    return redirect('/{}'.format(user))



if __name__ == '__main__':
    app.run(debug=True)