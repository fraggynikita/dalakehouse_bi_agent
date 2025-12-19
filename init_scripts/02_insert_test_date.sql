INSERT INTO test.games_list (gameid, title, developers, publishers, genres, supported_languages, release_date)
VALUES
(3278740,'NEURO','[Revolt Games]','[Strategy First]','[Action]','[English, Russian]',DATE '2024-10-11'
);


INSERT INTO test.players (playerid, country, created)
VALUES
(76561198287452552, 'Brazil', TIMESTAMP '2016-03-02 06:14:20');


INSERT INTO test.game_prices (gameid, usd, eur, gbp, jpy, rub, date_acquired)
VALUES
(3278740, 5.99, 5.85, 5.10, 720.00, 228.00,DATE '2024-11-28');


INSERT INTO test.purchased_games (playerid, library)
VALUES
(76561199000909663,
 '[70,12120,12250,9480,32330,32450,21130,113200,240720,41070,730180,866800,564310,914110,320240,730,207610,218510,227300,200260,1003890,219150,1101790,219640,1190000,57740,359550,623990,1227530,205810,203160,206420,34270,220240,218620,1222140,250320,43110,1919590,261030,552500,249130,2239550,43160,108600,287630,287700,233290,311770,307690,324800,289650,371520,374900,341940,381210,418240,429570,453720,462960,463170,223100,394360,498240,516750,289070,292030,536220,557340,32440,675260]'
);


INSERT INTO test.reviews_s (reviewid, playerid, gameid, review, helpful, funny, awards, posted)
VALUES
(639549, 76561198399037664, 578080, '外挂游戏', 0, 0, 0,DATE '2019-11-11');


INSERT INTO test.games_description (
    name, short_description, long_description, genres,
    minimum_system_requirement, recommend_system_requirement,
    release_date, developer, publisher, overall_player_rating,
    number_of_reviews_from_purchased_people, number_of_english_reviews, link
)
VALUES
(
    'Cyberpunk 2077',
    'Cyberpunk 2077 is an open-world, action-adventure RPG set in the dark future of Night City — a dangerous megalopolis obsessed with power, glamor, and ceaseless body modification.',
    'About This Game
Cyberpunk 2077 is an open-world, action-adventure RPG set in the megalopolis of Night City, where you play as a cyberpunk mercenary wrapped up in a do-or-die fight for survival. Improved and featuring all-new free additional content, customize your character and playstyle as you take on jobs, build a reputation, and unlock upgrades. The relationships you forge and the choices you make will shape the story and the world around you. Legends are made here. What will yours be?IMMERSE YOURSELF WITH UPDATE 2.1Night City feels more alive than ever with the free Update 2.1! Take a ride on the fully functional NCART metro system, listen to music as you explore the city with the Radioport, hang out with your partner in V’s apartment, compete in replayable races, ride new vehicles, enjoy improved bike combat and handling, discover hiddens secrets and much, much more!CREATE YOUR OWN CYBERPUNKBecome an urban outlaw equipped with cybernetic enhancements and build your legend on the streets of Night City.EXPLORE THE CITY OF THE FUTURENight City is packed to the brim with things to do, places to see, and people to meet. And it’s up to you where to go, when to go, and how to get there.BUILD YOUR LEGENDGo on daring adventures and build relationships with unforgettable characters whose fates are shaped by the choices you make.EQUIPPED WITH IMPROVEMENTSExperience Cyberpunk 2077 with a host of changes and improvements to gameplay and economy, the city, map usage, and more.CLAIM EXCLUSIVE ITEMSClaim in-game swag & digital goodies inspired by CD PROJEKT RED games as part of the My Rewards program.GO TO CYBERPUNK.NET',
    '["Cyberpunk","Open World","Nudity","RPG","Singleplayer","Sci-fi","Futuristic","FPS","Mature","Story Rich","First-Person","Atmospheric","Exploration","Action","Violent","Great Soundtrack","Action RPG","Adventure","Character Customization","Immersive Sim"]',
    '["Requires a 64-bit processor and operating system","OS: 64-bit Windows 10","Processor: Core i7-6700 or Ryzen 5 1600","Memory: 12 GB RAM","Graphics: GeForce GTX 1060 6GB or Radeon RX 580 8GB or Arc A380","DirectX: Version 12","Storage: 70 GB available space","Additional Notes: SSD required. Attention: In this game you will encounter a variety of visual effects that may provide seizures or loss of consciousness in a minority of people. If you or someone you know experiences any of the above symptoms while playing, stop and seek medical attention immediately."]',
    '["Requires a 64-bit processor and operating system","OS: 64-bit Windows 10","Processor: Core i7-12700 or Ryzen 7 7800X3D","Memory: 16 GB RAM","Graphics: GeForce RTX 2060 SUPER or Radeon RX 5700 XT or Arc A770","DirectX: Version 12","Storage: 70 GB available space","Additional Notes: SSD required."]',
    '10 Dec, 2020',
    '["CD PROJEKT RED"]',
    '["CD PROJEKT RED"]',
    'Very Positive',
    '(680,264)',
    '324,124',
    'https://store.steampowered.com/app/1091500/Cyberpunk_2077?snr=1_category_4_action_salebrowseall'
);


INSERT INTO test.reviews_m (review, hours_played, helpful, funny, recommendation, "date", game_name, username)
VALUES
('The game is fun.', 16.7, 993, 58, 'Not Recommended', 'September 13', 'Warhammer 40,000: Space Marine 2', 'metroidtim');