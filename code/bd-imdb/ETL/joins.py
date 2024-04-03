def inner_join(df, df2, cond, df3= None ):
    df = df.join(df2, cond, 'inner')

    if df3 is not None:
        df = df.join(df3, cond, 'inner')

    return df

def outer_join(df, df2, cond, df3= None ):
    df = df.join(df2, cond, 'outer')

    if df3 is not None:
        df = df.join(df3, cond, 'outer')

    return df

def left_join(df, df2, cond, df3= None ):
    df = df.join(df2, cond, 'left')

    if df3 is not None:
        df = df.join(df3, cond, 'left')

    return df


def left_join_extra(df, df2):
    return df.join(df2,
                       (
                           (df['movie_name'] == df2['movie_name']) & 
                           (df['runtimeMinutes'] == df2['runtimeMinutes']) & 
                           (df['year'] == df2['year'])
                       ),
                       how='left'
                       ).drop(df2.movie_name).drop(df2.runtimeMinutes).drop(df2.year).drop_duplicates(subset=['movie_name', 'runtimeMinutes', 'year'])

