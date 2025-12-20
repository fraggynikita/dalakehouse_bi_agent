from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# DAG: datalake -> silver (Iceberg on S3) через Trino
# В silver поле genres сохраняем как строку в формате PostgreSQL TEXT[] literal: {"a","b"}

with DAG(
    dag_id="load_data_to_silver",
    schedule_interval="@once",
    start_date=datetime(1024, 1, 1),
    catchup=False,
    tags=["silver", "trino", "iceberg", "s3"],
) as dag:

    # 1) Создаем схему silver с явным location в S3
    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id="trino_def",
        sql="""
        CREATE SCHEMA IF NOT EXISTS iceberg.silver
        WITH (location = 's3://datalake/silver/');
        """,
        do_xcom_push=False,
    )

    # 2) Создаем целевую Iceberg-таблицу в S3 (genres как VARCHAR с PG-array literal)
    create_silver_table_games_list = SQLExecuteQueryOperator(
        task_id="create_silver_table_games_list",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.games_list (
          gameid varchar,
          game_name varchar,
          genres array(varchar),        -- PG TEXT[] literal: {"Action","RPG"}
          release_date date
        )
        WITH (
          format = 'PARQUET',
          location = 's3://datalake/silver/games_list/'
        );
        """,
        do_xcom_push=False,
    )

    # 3) Загрузка/трансформация datalake -> silver
    # pandas-аналоги:
    # - drop columns: просто не выбираем developers/publishers/supported_languages
    # - dropna(how='any'): WHERE ... IS NOT NULL
    # - genres: "['a','b']" -> '{"a","b"}'
    # - release_date: TRY_CAST(... AS date)
    # - rename title -> game_name
    load_silver_games_list = SQLExecuteQueryOperator(
        task_id="load_silver_games_list",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.games_list
        SELECT
          CAST(gameid AS varchar) AS gameid,
          CAST(title AS varchar)  AS game_name,

          -- Convert Python-list string to PostgreSQL TEXT[] literal string:
          -- "['Mythology', 'Action RPG']" -> '{"Mythology","Action RPG"}'
        TRY(
        CAST(
            json_parse(replace(genres, '''', '"'))
            AS array(varchar)
        )
        ) AS genres,

          TRY_CAST(release_date AS date) AS release_date
        FROM iceberg.datalake.games_list
        WHERE
          gameid IS NOT NULL
          AND title IS NOT NULL
          AND genres IS NOT NULL
          AND TRY(CAST(json_parse(replace(genres, '''', '"')) AS array(varchar))) IS NOT NULL;
        """,
        do_xcom_push=False,
    )

    create_silver_players = SQLExecuteQueryOperator(
        task_id="create_silver_players",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.players (
        playerid varchar,
        country  varchar
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/players/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_players = SQLExecuteQueryOperator(
        task_id="load_silver_players",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.players
        SELECT
        CAST(playerid AS varchar) AS playerid,
        CAST(country AS varchar)  AS country
        FROM iceberg.datalake.players
        WHERE
        playerid IS NOT NULL
        AND country IS NOT NULL;
        """,
        do_xcom_push=False,
    )

    create_silver_game_prices = SQLExecuteQueryOperator(
        task_id="create_silver_game_prices",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.game_prices (
        gameid varchar,
        usd    double
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/game_prices/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_game_prices = SQLExecuteQueryOperator(
        task_id="load_silver_game_prices",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.game_prices
        SELECT
        CAST(gameid AS varchar) AS gameid,
        CAST(replace(usd, ',', '') AS double) AS usd
        FROM (
        SELECT
            gameid,
            usd,
            row_number() OVER (
            PARTITION BY gameid
            ORDER BY TRY_CAST(date_acquired AS date) DESC
            ) AS rn
        FROM iceberg.datalake.game_prices
        WHERE
            gameid IS NOT NULL
            AND usd IS NOT NULL
            AND TRY_CAST(date_acquired AS date) IS NOT NULL
        ) t
        WHERE rn = 1;
        """,
        do_xcom_push=False,
    )

    create_silver_purchased_games = SQLExecuteQueryOperator(
        task_id="create_silver_purchased_games",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.purchased_games (
        playerid varchar,
        library  array(varchar)
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/purchased_games/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_purchased_games = SQLExecuteQueryOperator(
        task_id="load_silver_purchased_games",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.purchased_games
        SELECT
        CAST(playerid AS varchar) AS playerid,
        transform(
            CAST(json_parse(library) AS array(integer)),
            x -> CAST(x AS varchar)
        ) AS library
        FROM iceberg.datalake.purchased_games
        WHERE
        playerid IS NOT NULL
        AND library IS NOT NULL;
        """,
        do_xcom_push=False,
    )

    create_silver_reviews_s = SQLExecuteQueryOperator(
        task_id="create_silver_reviews_s",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.reviews_s (
        playerid    bigint,
        gameid      bigint,
        review      varchar,
        helpful     integer,
        funny       integer,
        posted_date date
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/reviews_s/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_reviews_s = SQLExecuteQueryOperator(
        task_id="load_silver_reviews_s",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.reviews_s
        SELECT
        CAST(playerid AS bigint)          AS playerid,
        CAST(gameid AS bigint)            AS gameid,
        CAST(review AS varchar)           AS review,
        CAST(helpful AS integer)          AS helpful,
        CAST(funny AS integer)            AS funny,
        TRY_CAST(posted AS date)          AS posted_date
        FROM iceberg.datalake.reviews_s
        WHERE
        playerid IS NOT NULL
        AND gameid IS NOT NULL
        AND review IS NOT NULL
        AND helpful IS NOT NULL
        AND funny IS NOT NULL
        AND TRY_CAST(posted AS date) IS NOT NULL;
        """,
        do_xcom_push=False,
    )

    create_silver_reviews_m = SQLExecuteQueryOperator(
        task_id="create_silver_reviews_m",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.reviews_m (
        hours_played   double,
        helpful        integer,
        funny          integer,
        recommended    boolean,
        game_name      varchar
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/reviews_m/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_reviews_m = SQLExecuteQueryOperator(
        task_id="load_silver_reviews_m",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.reviews_m
        SELECT
        CAST(replace(hours_played, ',', '') AS double)            AS hours_played,
        CAST(replace(helpful, ',', '') AS bigint)                AS helpful,
        CAST(replace(funny, ',', '') AS bigint)                  AS funny,
        CASE
            WHEN lower(trim(recommendation)) = 'recommended' THEN TRUE
            WHEN lower(trim(recommendation)) = 'not recommended' THEN FALSE
            ELSE NULL
        END                                                       AS recommended,
        CAST(game_name AS varchar)                                AS game_name
        FROM iceberg.datalake.reviews_m
        WHERE
        hours_played IS NOT NULL
        AND helpful IS NOT NULL
        AND funny IS NOT NULL
        AND game_name IS NOT NULL
        AND TRY_CAST(helpful AS bigint) <= 1000 
        AND TRY_CAST(funny AS bigint) <= 1000
        AND lower(trim(recommendation)) IN ('recommended', 'not recommended');
        """,
        do_xcom_push=False,
    )

    create_silver_games_description = SQLExecuteQueryOperator(
        task_id="create_silver_games_description",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.games_description (
        game_name              varchar,
        short_description      varchar,
        long_description       varchar,
        genres                 array(varchar),   -- PG TEXT[] literal: {"Action","RPG"}
        release_date           date,
        overall_player_rating  varchar
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/games_description/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_games_description = SQLExecuteQueryOperator(
        task_id="load_silver_games_description",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.games_description
        SELECT
        CAST(name AS varchar) AS game_name,
        CAST(short_description AS varchar) AS short_description,
        regexp_replace(CAST(long_description AS varchar), '^About This Game\\n\\t*', '') AS long_description,

        -- genres supports BOTH:
        -- 1) python-list string: ['A','B']  -> convert ' -> " then json_parse
        -- 2) json array string:  ["A","B"] -> json_parse directly
        TRY(
        CAST(
            json_parse(
            CASE
                WHEN substr(trim(genres), 1, 2) = '["' THEN trim(genres)
                ELSE replace(trim(genres), '''', '"')
            END
            ) AS array(varchar)
        )
        ) AS genres,

        TRY(CAST(date_parse(release_date, '%d %b, %Y') AS date)) AS release_date,
        CAST(overall_player_rating AS varchar) AS overall_player_rating
        FROM iceberg.datalake.games_description
        WHERE
        name IS NOT NULL
        AND short_description IS NOT NULL
        AND long_description IS NOT NULL
        AND genres IS NOT NULL
        AND overall_player_rating IS NOT NULL
        AND TRY(CAST(date_parse(release_date, '%d %b, %Y') AS date)) IS NOT NULL
        AND TRY(
        CAST(
            json_parse(
            CASE
                WHEN substr(trim(genres), 1, 2) = '["' THEN trim(genres)
                ELSE replace(trim(genres), '''', '"')
            END
            ) AS array(varchar)
        )
        ) IS NOT NULL
        ;
        """,
        do_xcom_push=False,
    )


    create_silver_reviews_l = SQLExecuteQueryOperator(
        task_id="create_silver_reviews_l",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.reviews_l (
        gameid                       varchar,
        game_name                    varchar,
        language                     varchar,
        review                       varchar,
        posted_date                  date,
        recommended                  boolean,
        helpful                      integer,
        funny                        integer,
        received_for_free            boolean,
        written_during_early_access  boolean,
        playerid                     varchar,
        hours_played                 double
        )
        WITH (
        format = 'PARQUET',
        location = 's3://datalake/silver/reviews_l/'
        );
        """,
        do_xcom_push=False,
    )

    load_silver_reviews_l_part_0 = SQLExecuteQueryOperator(
        task_id="load_silver_reviews_l_part_0",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.reviews_l
        SELECT
        CAST(app_id AS varchar)                         AS gameid,
        CAST(app_name AS varchar)                       AS game_name,
        CAST(language AS varchar)                       AS language,
        CAST(review AS varchar)                         AS review,
        CAST(from_unixtime(TRY_CAST(text_created AS bigint)) AS date) AS posted_date,
        CAST(recommended AS boolean)                    AS recommended,
        CAST(votes_helpful AS bigint)                   AS helpful,
        CAST(votes_funny AS bigint)                     AS funny,
        CAST(received_for_free AS boolean)              AS received_for_free,
        CAST(written_during_early_access AS boolean)    AS written_during_early_access,
        CAST(author_steamid AS varchar)                 AS playerid,
        CAST(author_playtime_at_review AS double)       AS hours_played
        FROM iceberg.datalake.reviews_l
        WHERE
        app_id IS NOT NULL
        AND app_name IS NOT NULL
        AND language IS NOT NULL
        AND review IS NOT NULL
        AND TRY_CAST(text_created AS bigint) IS NOT NULL
        AND TRY_CAST(votes_helpful AS bigint) <= 1000 
        AND TRY_CAST(votes_funny AS bigint) <= 1000
        AND votes_helpful IS NOT NULL
        AND votes_funny IS NOT NULL
        AND author_playtime_at_review IS NOT NULL
        AND recommended IS NOT NULL
        AND received_for_free IS NOT NULL
        AND written_during_early_access IS NOT NULL
        AND author_steamid IS NOT NULL
        AND mod(crc32(to_utf8(text_created)), 4) = 0;
        """,
        do_xcom_push=False,
    )

    load_silver_reviews_l_part_1 = SQLExecuteQueryOperator(
        task_id="load_silver_reviews_l_part_1",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.reviews_l
        SELECT
        CAST(app_id AS varchar)                         AS gameid,
        CAST(app_name AS varchar)                       AS game_name,
        CAST(language AS varchar)                       AS language,
        CAST(review AS varchar)                         AS review,
        CAST(from_unixtime(TRY_CAST(text_created AS bigint)) AS date) AS posted_date,
        CAST(recommended AS boolean)                    AS recommended,
        CAST(votes_helpful AS bigint)                   AS helpful,
        CAST(votes_funny AS bigint)                     AS funny,
        CAST(received_for_free AS boolean)              AS received_for_free,
        CAST(written_during_early_access AS boolean)    AS written_during_early_access,
        CAST(author_steamid AS varchar)                 AS playerid,
        CAST(author_playtime_at_review AS double)       AS hours_played
        FROM iceberg.datalake.reviews_l
        WHERE
        app_id IS NOT NULL
        AND app_name IS NOT NULL
        AND language IS NOT NULL
        AND review IS NOT NULL
        AND TRY_CAST(text_created AS bigint) IS NOT NULL
        AND TRY_CAST(votes_helpful AS bigint) <= 1000 
        AND TRY_CAST(votes_funny AS bigint) <= 1000
        AND votes_helpful IS NOT NULL
        AND votes_funny IS NOT NULL
        AND author_playtime_at_review IS NOT NULL
        AND recommended IS NOT NULL
        AND received_for_free IS NOT NULL
        AND written_during_early_access IS NOT NULL
        AND author_steamid IS NOT NULL
        AND mod(crc32(to_utf8(text_created)), 4) = 1;
        """,
        do_xcom_push=False,
    )    

    load_silver_reviews_l_part_2 = SQLExecuteQueryOperator(
        task_id="load_silver_reviews_l_part_2",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.reviews_l
        SELECT
        CAST(app_id AS varchar)                         AS gameid,
        CAST(app_name AS varchar)                       AS game_name,
        CAST(language AS varchar)                       AS language,
        CAST(review AS varchar)                         AS review,
        CAST(from_unixtime(TRY_CAST(text_created AS bigint)) AS date) AS posted_date,
        CAST(recommended AS boolean)                    AS recommended,
        CAST(votes_helpful AS bigint)                   AS helpful,
        CAST(votes_funny AS bigint)                     AS funny,
        CAST(received_for_free AS boolean)              AS received_for_free,
        CAST(written_during_early_access AS boolean)    AS written_during_early_access,
        CAST(author_steamid AS varchar)                 AS playerid,
        CAST(author_playtime_at_review AS double)       AS hours_played
        FROM iceberg.datalake.reviews_l
        WHERE
        app_id IS NOT NULL
        AND app_name IS NOT NULL
        AND language IS NOT NULL
        AND review IS NOT NULL
        AND TRY_CAST(text_created AS bigint) IS NOT NULL
        AND TRY_CAST(votes_helpful AS bigint) <= 1000 
        AND TRY_CAST(votes_funny AS bigint) <= 1000
        AND votes_helpful IS NOT NULL
        AND votes_funny IS NOT NULL
        AND author_playtime_at_review IS NOT NULL
        AND recommended IS NOT NULL
        AND received_for_free IS NOT NULL
        AND written_during_early_access IS NOT NULL
        AND author_steamid IS NOT NULL
        AND mod(crc32(to_utf8(text_created)), 4) = 2;
        """,
        do_xcom_push=False,
    )

    load_silver_reviews_l_part_3 = SQLExecuteQueryOperator(
        task_id="load_silver_reviews_l_part_3",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.silver.reviews_l
        SELECT
        CAST(app_id AS varchar)                         AS gameid,
        CAST(app_name AS varchar)                       AS game_name,
        CAST(language AS varchar)                       AS language,
        CAST(review AS varchar)                         AS review,
        CAST(from_unixtime(TRY_CAST(text_created AS bigint)) AS date) AS posted_date,
        CAST(recommended AS boolean)                    AS recommended,
        CAST(votes_helpful AS bigint)                   AS helpful,
        CAST(votes_funny AS bigint)                     AS funny,
        CAST(received_for_free AS boolean)              AS received_for_free,
        CAST(written_during_early_access AS boolean)    AS written_during_early_access,
        CAST(author_steamid AS varchar)                 AS playerid,
        CAST(author_playtime_at_review AS double)       AS hours_played
        FROM iceberg.datalake.reviews_l
        WHERE
        app_id IS NOT NULL
        AND app_name IS NOT NULL
        AND language IS NOT NULL
        AND review IS NOT NULL
        AND TRY_CAST(text_created AS bigint) IS NOT NULL
        AND TRY_CAST(votes_helpful AS bigint) <= 1000 
        AND TRY_CAST(votes_funny AS bigint) <= 1000
        AND votes_helpful IS NOT NULL
        AND votes_funny IS NOT NULL
        AND author_playtime_at_review IS NOT NULL
        AND recommended IS NOT NULL
        AND received_for_free IS NOT NULL
        AND written_during_early_access IS NOT NULL
        AND author_steamid IS NOT NULL
        AND mod(crc32(to_utf8(text_created)), 4) = 3;
        """,
        do_xcom_push=False,
    )



    (
        create_silver_schema
        >> create_silver_table_games_list
        >> load_silver_games_list
        >> create_silver_players
        >> load_silver_players
        >> create_silver_game_prices
        >> load_silver_game_prices
        >> create_silver_purchased_games
        >> load_silver_purchased_games
        >> create_silver_reviews_s
        >> load_silver_reviews_s
        >> create_silver_reviews_m
        >> load_silver_reviews_m
        >> create_silver_games_description
        >> load_silver_games_description
        >> create_silver_reviews_l
        >> load_silver_reviews_l_part_0
        >> load_silver_reviews_l_part_1
        >> load_silver_reviews_l_part_2
        >> load_silver_reviews_l_part_3
    )   