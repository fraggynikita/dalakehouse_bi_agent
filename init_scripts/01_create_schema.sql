-- =========================================================
-- Schema
-- =========================================================
CREATE SCHEMA IF NOT EXISTS test;

-- =========================================================
-- 1) games_list
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".games_list (
    gameid              TEXT,
    title               TEXT,
    developers          TEXT,               -- пример: ['Revolt Games']
    publishers          TEXT,               -- пример: ['Strategy First']
    genres              TEXT,               -- пример: ['Action']
    supported_languages TEXT,               -- пример: ['English', 'Russian']
    release_date        TEXT
);

-- =========================================================
-- 2) players
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".players (
    playerid TEXT,
    country  TEXT,
    created  TEXT
);

-- =========================================================
-- 3) game_prices
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".game_prices (
    gameid         TEXT,
    usd            TEXT,
    eur            TEXT,
    gbp            TEXT,
    jpy            TEXT,
    rub            TEXT,
    date_acquired  TEXT
);

-- =========================================================
-- 4) purchased_games
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".purchased_games (
    playerid TEXT,
    library  TEXT -- пример: [70, 12120, ...]
);

-- =========================================================
-- 5) reviews_s (короткие отзывы)
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".reviews_s (
    reviewid TEXT,
    playerid TEXT,
    gameid   TEXT,
    review   TEXT,
    helpful  TEXT,
    funny    TEXT,
    awards   TEXT,
    posted   TEXT
);

-- =========================================================
-- 6) games_description
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".games_description (
    name                                  TEXT,
    short_description                      TEXT,
    long_description                       TEXT,
    genres                                TEXT,
    minimum_system_requirement             TEXT,
    recommend_system_requirement           TEXT,
    release_date                           TEXT,     -- в примере: "10 Dec, 2020"
    developer                              TEXT,
    publisher                              TEXT,
    overall_player_rating                  TEXT,     -- например: "Very Positive"
    number_of_reviews_from_purchased_people TEXT,    -- например: "(680,264)"
    number_of_english_reviews              TEXT,     -- например: "324,124"
    link                                  TEXT
);

-- =========================================================
-- 7) reviews_m (средний датасет отзывов)
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".reviews_m (
    review          TEXT,
    hours_played    TEXT,
    helpful         TEXT,
    funny           TEXT,
    recommendation  TEXT,          -- например: "Not Recommended"
    date            TEXT,          -- пример: "September 13" (без года)
    game_name       TEXT,
    username        TEXT
);

-- =========================================================
-- 8) reviews_l (большой датасет отзывов: структура по dtype)
-- =========================================================
CREATE TABLE IF NOT EXISTS "test".reviews_l (
    row_id                              TEXT,   -- индекс из CSV (пустое имя)
    app_id                          TEXT,
    app_name                        TEXT,
    review_id                       TEXT,
    language                        TEXT,
    review                          TEXT,
    TEXT_created                    TEXT,
    TEXT_updated                    TEXT,
    recommended                     TEXT,
    votes_helpful                   TEXT,
    votes_funny                     TEXT,
    weighted_vote_score             TEXT,
    comment_count                   TEXT,
    steam_purchase                  TEXT,
    received_for_free               TEXT,
    written_during_early_access     TEXT,
    author_steamid                  TEXT,
    author_num_games_owned          TEXT,
    author_num_reviews              TEXT,
    author_playtime_forever         TEXT,
    author_playtime_last_two_weeks  TEXT,
    author_playtime_at_review       TEXT,
    author_last_played              TEXT
);


