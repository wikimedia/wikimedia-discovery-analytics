import itertools

import mw_sql_to_hive


def test_get_db_mapping():
    db_mapping = mw_sql_to_hive._get_mediawiki_section_dbname_mapping([
        ('/path/to/s1.dblist', ['# blah blah', 'foowiki', 'barwiki']),
        ('path/to/s2.dblist', ['# other', 'pytest # comment', ''])
    ])

    assert {'foowiki', 'barwiki', 'pytest'} == set(db_mapping.keys())
    assert db_mapping['foowiki'] == 's1'
    assert db_mapping['barwiki'] == 's1'
    assert db_mapping['pytest'] == 's2'


def test_get_mysql_user_pass():
    content = """# Note: Things

[client]
#host =
password = abcdefg%12345
#port =
user = pytestuser
other_stuff = 64M"""
    user, password = mw_sql_to_hive.get_mysql_options_file_user_pass(content)
    assert (user, password) == ('pytestuser', 'abcdefg%12345')


def test_union_all_happy_path(spark):
    num_dfs = 4
    num_rows = 5
    dfs = [spark.range(num_rows) for _ in range(num_dfs)]
    rows = mw_sql_to_hive.union_all_df(dfs).collect()
    assert len(rows) == 20
    # Chain is equiv to union
    expect = itertools.chain(*[range(num_rows) for _ in range(num_dfs)])
    assert sorted([r.id for r in rows]) == sorted(expect)
