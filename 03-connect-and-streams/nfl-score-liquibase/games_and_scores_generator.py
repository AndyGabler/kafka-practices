from datetime import date
from datetime import timedelta
import random

games_file = open("./src/main/resources/db/changelog/2026-02-13/games.csv", 'w')
scores_file = open("./src/main/resources/db/changelog/2026-02-13/scores.csv", 'w')

game_count = 25
touchdowns_per_game = 4
field_goals_per_game = 2

game_date = date.fromisoformat("2025-09-07")

teams = [
    {
        "name": "Bears",
        "quarterback": "Caleb Williams",
        "kicker": "Cairo Santos",
        "receivers": ["DJ Moore", "Keenan Allen", "D'Andre Swift"]
    },
    {
        "name": "Bengals",
        "quarterback": "Joe Flacco",
        "kicker": "Evan McPherson",
        "receivers": ["Ja'Marr Chase", "Tee Higgins", "Joe Mixon"]
    },
    {
        "name": "Bills",
        "quarterback": "Josh Allen",
        "kicker": "Matt Prater",
        "receivers": ["Stefon Diggs", "James Cook", "Dalton Kincaid"]
    },
    {
        "name": "Broncos",
        "quarterback": "Bo Nix",
        "kicker": "Wil Lutz",
        "receivers": ["Courtland Sutton", "Javonte Williams", "Marvin Mims"]
    },
    {
        "name": "Browns",
        "quarterback": "Shedeur Sanders",
        "kicker": "Andre Szmyt",
        "receivers": ["Nick Chubb", "Amari Cooper", "Jerry Jeudy"]
    },
    {
        "name": "Buccaneers",
        "quarterback": "Baker Mayfield",
        "kicker": "Chase McLaughlin",
        "receivers": ["Mike Evans", "Chris Godwin", "Rachaad White"]
    },
    {
        "name": "Cardinals",
        "quarterback": "Kyler Murray",
        "kicker": "Chad Ryland",
        "receivers": ["James Conner", "Marvin Harrison Jr.", "Trey McBride"]
    },
    {
        "name": "Chargers",
        "quarterback": "Justin Herbert",
        "kicker": "Cameron Dicker",
        "receivers": ["Keenan Allen", "Austin Ekeler", "Quentin Johnston"]
    },
    {
        "name": "Chiefs",
        "quarterback": "Patrick Mahomes",
        "kicker": "Harrison Butker",
        "receivers": ["Travis Kelce", "Isiah Pacheco", "Rashee Rice"]
    },
    {
        "name": "Colts",
        "quarterback": "Daniel Jones",
        "kicker": "Blake Grupe",
        "receivers": ["Jonathan Taylor", "Michael Pittman Jr.", "Josh Downs"]
    },
    {
        "name": "Commanders",
        "quarterback": "Jaden Daniels",
        "kicker": "Jake Moody",
        "receivers": ["Terry McLaurin", "Brian Robinson Jr.", "Jahan Dotson"]
    },
    {
        "name": "Cowboys",
        "quarterback": "Dak Prescott",
        "kicker": "Brandon Aubrey",
        "receivers": ["CeeDee Lamb", "Tony Pollard", "Brandin Cooks"]
    },
    {
        "name": "Dolphins",
        "quarterback": "Tua Tagovailoa",
        "kicker": "Riley Patterson",
        "receivers": ["Tyreek Hill", "Jaylen Waddle", "Raheem Mostert"]
    },
    {
        "name": "Eagles",
        "quarterback": "Jalen Hurts",
        "kicker": "Jake Elliott",
        "receivers": ["AJ Brown", "DeVonta Smith", "Saquon Barkley"]
    },
    {
        "name": "Falcons",
        "quarterback": "Kirk Cousins",
        "kicker": "Zane Gonzalez",
        "receivers": ["Bijan Robinson", "Drake London", "Kyle Pitts"]
    },
    {
        "name": "FortyNiners",
        "quarterback": "Brock Purdy",
        "kicker": "Eddy Pineiro",
        "receivers": ["Christian McCaffrey", "Brandon Aiyuk", "Deebo Samuel"]
    },
    {
        "name": "Giants",
        "quarterback": "Jaxson Dart",
        "kicker": "Ben Sauls",
        "receivers": ["Malik Nabers", "Saquon Barkley", "Darius Slayton"]
    },
    {
        "name": "Jaguars",
        "quarterback": "Trevor Lawrence",
        "kicker": "Cam Little",
        "receivers": ["Travis Etienne", "Christian Kirk", "Brian Thomas Jr."]
    },
    {
        "name": "Jets",
        "quarterback": "Justin Fields",
        "kicker": "Nick Folk",
        "receivers": ["Garrett Wilson", "Breece Hall", "Mike Williams"]
    },
    {
        "name": "Lions",
        "quarterback": "Jared Goff",
        "kicker": "Jake Bates",
        "receivers": ["Amon-Ra St. Brown", "Jahmyr Gibbs", "Sam LaPorta"]
    },
    {
        "name": "Packers",
        "quarterback": "Jordan Love",
        "kicker": "Brandon McManus",
        "receivers": ["Josh Jacobs", "Christian Watson", "Jayden Reed"]
    },
    {
        "name": "Panthers",
        "quarterback": "Bryce Young",
        "kicker": "Ryan Fitzgerald",
        "receivers": ["Adam Thielen", "Chuba Hubbard", "Diontae Johnson"]
    },
    {
        "name": "Patriots",
        "quarterback": "Drake Maye",
        "kicker": "Andres Borregales",
        "receivers": ["Rhamondre Stevenson", "Demario Douglas", "Ja'Lynn Polk"]
    },
    {
        "name": "Raiders",
        "quarterback": "Geno Smith",
        "kicker": "Daniel Carlson",
        "receivers": ["Davante Adams", "Josh Jacobs", "Jakobi Meyers"]
    },
    {
        "name": "Rams",
        "quarterback": "Matthew Stafford",
        "kicker": "Harrison Mevis",
        "receivers": ["Puka Nacua", "Cooper Kupp", "Kyren Williams"]
    },
    {
        "name": "Ravens",
        "quarterback": "Lamar Jackson",
        "kicker": "Tyler Loop",
        "receivers": ["Mark Andrews", "Zay Flowers", "Derrick Henry"]
    },
    {
        "name": "Saints",
        "quarterback": "Tyler Shough",
        "kicker": "Charlie Smyth",
        "receivers": ["Chris Olave", "Alvin Kamara", "Rashid Shaheed"]
    },
    {
        "name": "Seahawks",
        "quarterback": "Sam Darnold",
        "kicker": "Jason Myers",
        "receivers": ["DK Metcalf", "Kenneth Walker III", "Jaxon Smith-Njigba"]
    },
    {
        "name": "Steelers",
        "quarterback": "Aaron Rodgers",
        "kicker": "Chris Boswell",
        "receivers": ["Najee Harris", "George Pickens", "Jaylen Warren"]
    },
    {
        "name": "Texans",
        "quarterback": "CJ Stroud",
        "kicker": "Ka'imi Fairbairn",
        "receivers": ["Nico Collins", "Tank Dell", "Joe Mixon"]
    },
    {
        "name": "Titans",
        "quarterback": "Cameron Ward",
        "kicker": "Joey Slye",
        "receivers": ["DeAndre Hopkins", "Tony Pollard", "Calvin Ridley"]
    },
    {
        "name": "Vikings",
        "quarterback": "JJ McCarthy",
        "kicker": "Will Reichard",
        "receivers": ["Justin Jefferson", "Jordan Addison", "Aaron Jones"]
    }
]

games = []
scores = []
while len(games) < game_count:
    if random.randint(0, 4) == 4:
        game_date = game_date + timedelta(days=5)
    visiting_team = teams[random.randint(0, len(teams) - 1)]
    home_team = teams[random.randint(0, len(teams) - 1)]
    
    if visiting_team["name"] == home_team["name"]:
        continue
    
    game_id = str(len(games) + 1)
    games.append([game_id, game_date.isoformat(), home_team["name"], visiting_team["name"]])
    
    touchdowns_to_add = touchdowns_per_game + random.randint(-3, 3)
    field_goals_to_add = field_goals_per_game + random.randint(-2, 2)
    
    while touchdowns_to_add > 0:
        team = random.choice([visiting_team, home_team])
        snap_type = random.choice(["PASSING", "RUSHING"])
        ball_carrier = random.choice(team["receivers"])
        
        score_to_add = [game_id, team["quarterback"], ball_carrier, snap_type, team["name"]]
        scores.append(score_to_add)
        
        conversion_type = random.choices(["EXTRA POINT KICK", "NO POINT", "2-POINT CONVERSION"], weights=[80, 15, 5], k=1)[0]
        if conversion_type == "EXTRA POINT KICK":
            scores.append([game_id, team["quarterback"], team["kicker"], conversion_type, team["name"]])
        elif conversion_type == "2-POINT CONVERSION":
            scores.append([game_id, team["quarterback"], ball_carrier, conversion_type, team["name"]])
        
        touchdowns_to_add -= 1
    
    while field_goals_to_add > 0:
        team = random.choice([visiting_team, home_team])
        scores.append([game_id, team["quarterback"], team["kicker"], "FIELD GOAL", team["name"]])
        
        field_goals_to_add -= 1

games_csv_text = "id,matchDate,visitingTeam,homeTeam"
for game in games:
    games_csv_text = games_csv_text + "\n" + game[0] + "," + game[1] + "," + game[2] + "," + game[3]
games_file.write(games_csv_text)

scores_csv_text = "gameId,quarterback,ballCarrier,snapType,team"
for score in scores:
    scores_csv_text = scores_csv_text + "\n" + score[0] + "," + score[1] + "," + score[2] + "," + score[3] + "," + score[4]
scores_file.write(scores_csv_text)