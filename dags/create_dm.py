from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="create_gold_dm",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "trino", "iceberg", "games", "dm"],
) as dag:

    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="trino_def",
        sql="""
        CREATE SCHEMA IF NOT EXISTS iceberg.gold
        WITH (location = 's3://datalake/gold/');
        """,
        do_xcom_push=False,
    )

    create_dm_market_games_overview = SQLExecuteQueryOperator(
        task_id="create_dm_market_games_overview",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.gold.dm_market_games_overview (
            gameid               varchar,
            game_name            varchar,
            release_date         date,
            price_usd            double,
            genres               array(varchar),
            reviews_total_cnt    bigint,
            reviews_positive_cnt bigint,
            reviews_negative_cnt bigint,
            recommendation_rate  double,
            avg_hours_played     double,
            players_total_cnt    bigint
        )
        WITH (
            format = 'PARQUET',
            location = 's3://datalake/gold/dm_market_games_overview/'
        );
        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.gameid
        IS 'Уникальный идентификатор игры в Steam';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.game_name
        IS 'Наименование игры';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.release_date
        IS 'Дата релиза игры';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.price_usd
        IS 'Стоимость игры в долларах США';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.genres
        IS 'Список жанров игры';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.reviews_total_cnt
        IS 'Общее количество отзывов по игре';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.reviews_positive_cnt
        IS 'Количество положительных отзывов';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.reviews_negative_cnt
        IS 'Количество отрицательных отзывов';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.recommendation_rate
        IS 'Доля положительных рекомендаций пользователей';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.avg_hours_played
        IS 'Среднее количество часов, проведённых игроками в игре';

        COMMENT ON COLUMN iceberg.gold.dm_market_games_overview.players_total_cnt
        IS 'Общее количество пользователей, оставивших отзывы';
        
        """,
        do_xcom_push=False,
    )

    insert_dm_market_games_overview = SQLExecuteQueryOperator(
        task_id="insert_dm_market_games_overview",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.gold.dm_market_games_overview
        WITH games_dim AS (
            SELECT
                gameid,
                game_name,
                release_date,
                genres,
                lower(trim(regexp_replace(game_name, '\\\\s+', ' '))) AS game_name_key
            FROM iceberg.silver.games_list
            WHERE
                gameid IS NOT NULL
                AND game_name IS NOT NULL
                AND release_date IS NOT NULL
                AND genres IS NOT NULL
        ),

        reviews_all AS (
            -- reviews_l (минуты -> часы)
            SELECT
                g.gameid              AS gameid,
                r.recommended         AS recommended,
                r.hours_played / 60.0 AS hours_played
            FROM iceberg.silver.reviews_l r
            JOIN games_dim g
                ON r.gameid = g.gameid
            WHERE
                r.recommended IS NOT NULL
                AND r.hours_played IS NOT NULL

            UNION ALL

            -- reviews_m (gameid через games_list + минуты -> часы)
            SELECT
                g.gameid               AS gameid,
                rm.recommended          AS recommended,
                rm.hours_played / 60.0  AS hours_played
            FROM iceberg.silver.reviews_m rm
            JOIN games_dim g
                ON lower(trim(regexp_replace(rm.game_name, '\\\\s+', ' '))) = g.game_name_key
            WHERE
                rm.game_name IS NOT NULL
                AND rm.recommended IS NOT NULL
                AND rm.hours_played IS NOT NULL
        )

        SELECT
            g.gameid                                    AS gameid,
            g.game_name                                 AS game_name,
            g.release_date                              AS release_date,
            gp.usd                                      AS price_usd,
            g.genres                                    AS genres,

            COUNT(*)                                    AS reviews_total_cnt,
            SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END)
                                                        AS reviews_positive_cnt,
            SUM(CASE WHEN NOT r.recommended THEN 1 ELSE 0 END)
                                                        AS reviews_negative_cnt,

            CAST(SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END) AS DOUBLE)
              / NULLIF(COUNT(*), 0)                      AS recommendation_rate,

            AVG(r.hours_played)                          AS avg_hours_played,

            -- по твоему правилу
            COUNT(*)                                    AS players_total_cnt

        FROM games_dim g
        JOIN iceberg.silver.game_prices gp
            ON g.gameid = gp.gameid
        JOIN reviews_all r
            ON g.gameid = r.gameid

        WHERE
            gp.usd IS NOT NULL

        GROUP BY
            g.gameid,
            g.game_name,
            g.release_date,
            gp.usd,
            g.genres

        HAVING
            COUNT(*) > 0
            AND AVG(r.hours_played) IS NOT NULL;
        """,
        do_xcom_push=False,
    )

    create_dm_player_engagement = SQLExecuteQueryOperator(
        task_id="create_dm_player_engagement",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.gold.dm_player_engagement (
            playerid             varchar,
            country              varchar,
            games_reviewed_cnt   bigint,
            total_hours_played   double,
            avg_hours_per_game   double,
            reviews_total_cnt    bigint,
            recommendation_rate  double,
            helpful_votes_total  bigint,
            funny_votes_total    bigint
        )
        WITH (format = 'PARQUET',
            location = 's3://datalake/gold/dm_dm_player_engagement/');
        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.playerid
        IS 'Уникальный идентификатор игрока Steam';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.country
        IS 'Страна проживания игрока';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.games_reviewed_cnt
        IS 'Количество игр, по которым игрок оставил отзывы';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.total_hours_played
        IS 'Суммарное количество часов, проведённых игроком в играх';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.avg_hours_per_game
        IS 'Среднее количество часов игры на одну игру с отзывом';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.reviews_total_cnt
        IS 'Общее количество отзывов, оставленных игроком';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.recommendation_rate
        IS 'Доля положительных рекомендаций игрока';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.helpful_votes_total
        IS 'Суммарное количество голосов «полезно» по отзывам игрока';

        COMMENT ON COLUMN iceberg.gold.dm_player_engagement.funny_votes_total
        IS 'Суммарное количество голосов «смешно» по отзывам игрока';

        """,
        do_xcom_push=False,
    )

    # 4) Загружаем данные (INSERT)
    insert_dm_player_engagement = SQLExecuteQueryOperator(
        task_id="insert_dm_player_engagement",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.gold.dm_player_engagement
        WITH
        players_base AS (
            SELECT
                playerid,
                any_value(country) AS country
            FROM iceberg.silver.players
            WHERE playerid IS NOT NULL
            GROUP BY playerid
        ),

        reviews_player_agg AS (
            SELECT
                r.playerid,

                COUNT(*)                           AS reviews_total_cnt,
                COUNT(DISTINCT r.gameid)           AS games_reviewed_cnt,
                SUM(r.hours_played / 60.0)         AS total_hours_played,

                CAST(
                    SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END)
                    AS DOUBLE
                ) / NULLIF(COUNT(*), 0)            AS recommendation_rate,

                SUM(r.helpful)                     AS helpful_votes_total,
                SUM(r.funny)                       AS funny_votes_total
            FROM iceberg.silver.reviews_l r
            WHERE
                r.playerid IS NOT NULL
                AND r.gameid IS NOT NULL
                AND r.hours_played IS NOT NULL
                AND r.recommended IS NOT NULL
                AND r.helpful IS NOT NULL
                AND r.funny IS NOT NULL
            GROUP BY r.playerid
        )

        SELECT
            p.playerid                                  AS playerid,
            p.country                                   AS country,

            rp.games_reviewed_cnt                       AS games_reviewed_cnt,
            rp.total_hours_played                       AS total_hours_played,

            rp.total_hours_played / rp.games_reviewed_cnt
                                                        AS avg_hours_per_game,

            rp.reviews_total_cnt                        AS reviews_total_cnt,
            rp.recommendation_rate                      AS recommendation_rate,
            rp.helpful_votes_total                      AS helpful_votes_total,
            rp.funny_votes_total                        AS funny_votes_total

        FROM players_base p
        JOIN reviews_player_agg rp
            ON p.playerid = rp.playerid
        WHERE
            rp.reviews_total_cnt > 0;
        """,
        do_xcom_push=False,
    )

    create_dm_genre_performance = SQLExecuteQueryOperator(
        task_id="create_dm_genre_performance",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.gold.dm_genre_performance (
            genre                 varchar,
            games_cnt             bigint,
            avg_price             double,
            players_total_cnt     bigint,
            recommendation_rate   double,
            avg_hours_played      double
        )
        WITH (format = 'PARQUET',
            location = 's3://datalake/gold/dm_genre_performance/');
        COMMENT ON COLUMN iceberg.gold.dm_genre_performance.genre
        IS 'Жанр видеоигр';

        COMMENT ON COLUMN iceberg.gold.dm_genre_performance.games_cnt
        IS 'Количество игр в данном жанре';

        COMMENT ON COLUMN iceberg.gold.dm_genre_performance.avg_price
        IS 'Средняя стоимость игр в жанре';

        COMMENT ON COLUMN iceberg.gold.dm_genre_performance.players_total_cnt
        IS 'Общее количество игроков, оставивших отзывы по жанру';

        COMMENT ON COLUMN iceberg.gold.dm_genre_performance.recommendation_rate
        IS 'Средняя доля положительных рекомендаций по жанру';

        COMMENT ON COLUMN iceberg.gold.dm_genre_performance.avg_hours_played
        IS 'Среднее количество часов, проведённых в играх жанра';

        """,
        do_xcom_push=False,
    )


    # 4) Загружаем данные (INSERT)
    insert_dm_genre_performance = SQLExecuteQueryOperator(
        task_id="insert_dm_genre_performance",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.gold.dm_genre_performance
        WITH
        games_dim AS (
            SELECT
                gl.gameid,
                gl.game_name,
                gp.usd AS price_usd,
                COALESCE(gd.genres, gl.genres) AS genres,
                lower(trim(regexp_replace(gl.game_name, '\\s+', ' '))) AS game_name_key
            FROM iceberg.silver.games_list gl
            LEFT JOIN iceberg.silver.games_description gd
                ON lower(trim(regexp_replace(gl.game_name, '\\s+', ' '))) =
                   lower(trim(regexp_replace(gd.game_name, '\\s+', ' ')))
            JOIN iceberg.silver.game_prices gp
                ON gl.gameid = gp.gameid
            WHERE gl.gameid IS NOT NULL
              AND gl.game_name IS NOT NULL
              AND gp.usd IS NOT NULL
              AND COALESCE(gd.genres, gl.genres) IS NOT NULL
        ),

        games_genres AS (
            SELECT
                g.gameid,
                g.price_usd,
                genre
            FROM games_dim g
            CROSS JOIN UNNEST(g.genres) AS t(genre)
            WHERE genre IS NOT NULL
        ),

        reviews_l_agg AS (
            SELECT
                r.gameid AS gameid,
                COUNT(*) AS reviews_cnt,
                SUM(CASE WHEN r.recommended THEN 1 ELSE 0 END) AS pos_cnt,
                SUM(r.hours_played / 60.0) AS sum_hours,
                approx_set(r.playerid) AS players_hll
            FROM iceberg.silver.reviews_l r
            WHERE r.gameid IS NOT NULL
              AND r.playerid IS NOT NULL
              AND r.recommended IS NOT NULL
              AND r.hours_played IS NOT NULL
            GROUP BY r.gameid
        ),

        reviews_m_agg AS (
            SELECT
                g.gameid AS gameid,
                COUNT(*) AS reviews_cnt,
                SUM(CASE WHEN rm.recommended THEN 1 ELSE 0 END) AS pos_cnt,
                SUM(rm.hours_played) AS sum_hours
            FROM iceberg.silver.reviews_m rm
            JOIN games_dim g
              ON lower(trim(regexp_replace(rm.game_name, '\\s+', ' '))) = g.game_name_key
            WHERE rm.game_name IS NOT NULL
              AND rm.recommended IS NOT NULL
              AND rm.hours_played IS NOT NULL
            GROUP BY g.gameid
        ),

        game_reviews_agg AS (
            SELECT
                gameid,
                SUM(reviews_cnt) AS reviews_cnt,
                SUM(pos_cnt)     AS pos_cnt,
                SUM(sum_hours)   AS sum_hours
            FROM (
                SELECT gameid, reviews_cnt, pos_cnt, sum_hours FROM reviews_l_agg
                UNION ALL
                SELECT gameid, reviews_cnt, pos_cnt, sum_hours FROM reviews_m_agg
            ) t
            GROUP BY gameid
        )

        SELECT
            gg.genre AS genre,

            COUNT(DISTINCT gg.gameid) AS games_cnt,
            AVG(gg.price_usd)         AS avg_price,

            SUM(gr.reviews_cnt)       AS players_total_cnt,

            CAST(SUM(gr.pos_cnt) AS DOUBLE) / NULLIF(SUM(gr.reviews_cnt), 0)
                                   AS recommendation_rate,

            CAST(SUM(gr.sum_hours) AS DOUBLE) / NULLIF(SUM(gr.reviews_cnt), 0)
                                   AS avg_hours_played

        FROM games_genres gg
        JOIN game_reviews_agg gr
          ON gg.gameid = gr.gameid
        LEFT JOIN reviews_l_agg rl
          ON gg.gameid = rl.gameid
        GROUP BY gg.genre
        HAVING SUM(gr.reviews_cnt) > 0;
        """,
        do_xcom_push=False,
    )

    create_dm_price_vs_feedback = SQLExecuteQueryOperator(
        task_id="create_dm_price_vs_feedback",
        conn_id="trino_def",
        sql="""
        CREATE TABLE IF NOT EXISTS iceberg.gold.dm_price_vs_feedback (
            price_bucket         varchar,
            games_cnt            bigint,
            avg_rating           double,
            recommendation_rate  double,
            avg_hours_played     double,
            reviews_total_cnt    bigint
        )
        WITH (format = 'PARQUET',
            location = 's3://datalake/gold/dm_price_vs_feedback/');
        COMMENT ON COLUMN iceberg.gold.dm_price_vs_feedback.price_bucket
        IS 'Ценовой диапазон игры (бакет стоимости)';

        COMMENT ON COLUMN iceberg.gold.dm_price_vs_feedback.games_cnt
        IS 'Количество игр в данном ценовом диапазоне';

        COMMENT ON COLUMN iceberg.gold.dm_price_vs_feedback.avg_rating
        IS 'Средний уровень пользовательской оценки в ценовом диапазоне';

        COMMENT ON COLUMN iceberg.gold.dm_price_vs_feedback.recommendation_rate
        IS 'Средняя доля положительных рекомендаций пользователей';

        COMMENT ON COLUMN iceberg.gold.dm_price_vs_feedback.avg_hours_played
        IS 'Среднее количество часов, проведённых игроками';

        COMMENT ON COLUMN iceberg.gold.dm_price_vs_feedback.reviews_total_cnt
        IS 'Общее количество отзывов в ценовом диапазоне';

        """,
        do_xcom_push=False,
    )

    insert_dm_price_vs_feedback = SQLExecuteQueryOperator(
        task_id="insert_dm_price_vs_feedback",
        conn_id="trino_def",
        sql="""
        INSERT INTO iceberg.gold.dm_price_vs_feedback
        SELECT
            price_bucket,

            COUNT(DISTINCT gameid)            AS games_cnt,

            AVG(recommendation_rate)          AS avg_rating,
            AVG(recommendation_rate)          AS recommendation_rate,

            AVG(avg_hours_played)             AS avg_hours_played,

            SUM(reviews_total_cnt)            AS reviews_total_cnt

        FROM (
            SELECT
                gameid,
                reviews_total_cnt,
                recommendation_rate,
                avg_hours_played,

                CASE
                    WHEN price_usd >= 0  AND price_usd < 10  THEN '0-10'
                    WHEN price_usd >= 10 AND price_usd < 20  THEN '10-20'
                    WHEN price_usd >= 20 AND price_usd < 30  THEN '20-30'
                    ELSE '30+'
                END AS price_bucket
            FROM iceberg.gold.dm_market_games_overview
            WHERE
                price_usd IS NOT NULL
                AND reviews_total_cnt > 0
        ) t

        GROUP BY price_bucket

        ORDER BY
            CASE price_bucket
                WHEN '0-10' THEN 1
                WHEN '10-20' THEN 2
                WHEN '20-30' THEN 3
                ELSE 4
            END
        """,
        do_xcom_push=False,
    )

    (
        create_gold_schema 
        >> create_dm_market_games_overview 
        >> insert_dm_market_games_overview 
        >> create_dm_player_engagement 
        >> insert_dm_player_engagement
        >> create_dm_genre_performance
        >> insert_dm_genre_performance
        >> create_dm_price_vs_feedback
        >> insert_dm_price_vs_feedback
    )
