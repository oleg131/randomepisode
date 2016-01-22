from flask import Flask, redirect

from sqlalchemy import create_engine
import pandas as pd

import requests
from unidecode import unidecode

app = Flask(__name__)



@app.route('/')
def index():
    return '<a href=\'/new/\'>Generate new user</a>'

@app.route('/new/')
def new_user():
    import random, string
    rnd = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(50))
    return redirect('/{}/edit'.format(rnd), code=302)

@app.route('/<user>/edit', methods=['GET', 'POST'])
def edit(user):
    from flask import request, url_for
    from sqlalchemy.sql.expression import insert

    engine = create_engine('mysql+mysqldb://user:pass@127.0.0.1/db')

    if request.method == 'POST':
        show = request.form['show']

        r = requests.get('http://api.tvmaze.com/singlesearch/shows?q={}'.format(show))
        if r:
            title = r.json()['name']
            title = unidecode(title)

            statement = '''
            SELECT * FROM users
            JOIN shows using(show_id)
            WHERE hash = '{}'
            AND title = '{}'
            '''.format(user, title)
            df = pd.read_sql(statement, engine)

            if len(df)==0:

                def get_show_by_title(title):
                    statement = '''
                    SELECT * FROM shows
                    WHERE title = '{}'
                    '''.format(title)
                    df = pd.read_sql(statement, engine)
                    if len(df)!=0:
                        return df['show_id'].iloc[0], df['title'].iloc[0]
                    else:
                        return None

                show = get_show_by_title(title)

                conn = engine.connect()

                if show:
                    trans = conn.begin()
                    statement = '''INSERT INTO `users` (`user_id`, `hash`, `show_id`) VALUES (NULL, '{}', '{}');'''.format(user, show[0])
                    result = conn.execute(statement)
                    trans.commit()
                else:
                    trans = conn.begin()
                    statement = '''INSERT INTO `shows` (`show_id`, `title`) VALUES (NULL, '{}');'''.format(title)
                    result = conn.execute(statement)
                    trans.commit()

                    trans = conn.begin()
                    show_id,_ = get_show_by_title(title)
                    statement = '''INSERT INTO `users` (`user_id`, `hash`, `show_id`) VALUES (NULL, '{}', '{}');'''.format(user, show_id)
                    result = conn.execute(statement)
                    trans.commit()

        else:
            pass

        return redirect('/{}/edit'.format(user), code=302)

    else:
        out = '''
        <form method='post'>
        Add show:  <input type="text" name="show" value=""><input type="submit" value="Add">
        </form>
        '''
        statement = '''
        SELECT * FROM users
        JOIN shows using(show_id)
        WHERE hash = '{}'
        '''.format(user)
        df = pd.read_sql(statement, engine)

        out = out + '<br>' + 'Added shows <br>' + df[['title']].drop_duplicates().to_html()

        return out


@app.route('/<user>')
def users(user):
    engine = create_engine('mysql+mysqldb://user:pass@127.0.0.1/db')

    statement = '''
    SELECT * FROM users
    JOIN shows using(show_id)
    WHERE hash = '{}'
    '''.format(user)
    df = pd.read_sql(statement, engine)
    user_shows = df.drop_duplicates()
    
    statement = '''
    SELECT * FROM episodes
    JOIN shows using(show_id)
    JOIN users using(show_id)
    WHERE users.hash = '{}'
    '''.format(user)
    user_episodes = pd.read_sql(statement, engine)

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
        
        nonwatched.append(merged)

    nonwatched = pd.concat(nonwatched)

    random = nonwatched.sample(1)

    random['user'] = user

    random[['user', 'show_id', 'season', 'episode']].to_sql('episodes', engine, if_exists='append', index=False)

    out = '<h1>Watch<br> {} <br> {}x{}</h1>'.format(str(random['title'].iloc[0]), str(random['season'].iloc[0]), str(random['episode'].iloc[0]))

    return out






if __name__ == '__main__':
    app.run(debug=True)