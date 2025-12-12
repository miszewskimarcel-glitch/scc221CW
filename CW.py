import mysql.connector
import csv
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"

DB_NAME = "cs_tournament"


def connect():
    return mysql.connector.connect(
        host="",
        user="",
        password=""
    )


def create_database(conn, db_name):
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cur.close()


def use_database(conn, db_name):
    conn.database = db_name


def drop_tables(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS Performance")
    cur.execute("DROP TABLE IF EXISTS `Match`")
    cur.execute("DROP TABLE IF EXISTS Player")
    cur.execute("DROP TABLE IF EXISTS Team")
    conn.commit()
    cur.close()


def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE Team (
            teamID INT PRIMARY KEY,
            name VARCHAR(100),
            region VARCHAR(50),
            ranking INT
        )
    """)

    cur.execute("""
        CREATE TABLE Player (
            playerID INT PRIMARY KEY,
            teamID INT,
            username VARCHAR(50),
            country VARCHAR(50),
            role VARCHAR(30),
            details TEXT,
            overall_rating DECIMAL(4,2),
            FOREIGN KEY (teamID) REFERENCES Team(teamID)
        )
    """)

    cur.execute("""
        CREATE TABLE `Match` (
            matchID INT PRIMARY KEY,
            map VARCHAR(50),
            date DATE,
            duration INT,
            type_info VARCHAR(50),
            match_type VARCHAR(30),
            stage VARCHAR(50),
            team1ID INT,
            team2ID INT,
            team1_score INT,
            team2_score INT
        )
    """)

    cur.execute("""
        CREATE TABLE Performance (
            matchID INT,
            playerID INT,
            kills INT,
            deaths INT,
            assists INT,
            rating DECIMAL(4,2),
            details TEXT,
            PRIMARY KEY (matchID, playerID)
        )
    """)

    conn.commit()
    cur.close()


def load_teams(conn):
    cur = conn.cursor()
    with open("teams.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(
                "INSERT INTO Team VALUES (%s,%s,%s,%s)", row
            )
    conn.commit()
    cur.close()


def load_players(conn):
    cur = conn.cursor()
    with open("players.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(
                "INSERT INTO Player VALUES (%s,%s,%s,%s,%s,%s,%s)", row
            )
    conn.commit()
    cur.close()


def load_matches(conn):
    cur = conn.cursor()
    with open("matches.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(
                "INSERT INTO `Match` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row
            )
    conn.commit()
    cur.close()


def load_performance(conn):
    cur = conn.cursor()
    with open("performance.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(
                "INSERT INTO Performance VALUES (%s,%s,%s,%s,%s,%s,%s)", row
            )
    conn.commit()
    cur.close()


def q_top_players(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT p.username, AVG(perf.rating)
        FROM Player p
        JOIN Performance perf ON p.playerID = perf.playerID
        GROUP BY p.playerID, p.username
        ORDER BY AVG(perf.rating) DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def q_decisive_matches(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT matchID, map, team1_score, team2_score
        FROM `Match`
        WHERE ABS(team1_score - team2_score) >= 2
        ORDER BY matchID;
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def q_team_winrates(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.name,
            ROUND(
                100 * SUM(
                    CASE
                        WHEN (t.teamID = m.team1ID AND m.team1_score > m.team2_score)
                          OR (t.teamID = m.team2ID AND m.team2_score > m.team1_score)
                        THEN 1
                        ELSE 0
                    END
                ) / COUNT(*),
                2
            )
        FROM Team t
        JOIN `Match` m
            ON t.teamID = m.team1ID OR t.teamID = m.team2ID
        GROUP BY t.teamID, t.name
        ORDER BY 2 DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def q_player_count_by_country(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT country, COUNT(*)
        FROM Player
        WHERE country IS NOT NULL AND country <> ''
        GROUP BY country
        ORDER BY COUNT(*) DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def plot_bar(x, y, title, xlab, ylab):
    fig = go.Figure(go.Bar(x=x, y=y, text=y, textposition="outside"))
    fig.update_layout(title=title, xaxis_title=xlab, yaxis_title=ylab)
    fig.show()


def plot_pie(labels, values, title):
    fig = go.Figure(go.Pie(labels=labels, values=values, textinfo="label+percent"))
    fig.update_layout(title=title)
    fig.show()


if __name__ == "__main__":
    print("Connecting to database...")
    conn = connect()

    create_database(conn, DB_NAME)
    use_database(conn, DB_NAME)
    drop_tables(conn)
    create_tables(conn)

    print("Loading data from CSV files...")
    load_teams(conn)
    load_players(conn)
    load_matches(conn)
    load_performance(conn)

    print("Running queries...")
    top_players = q_top_players(conn)
    decisive = q_decisive_matches(conn)
    team_win = q_team_winrates(conn)
    country_counts = q_player_count_by_country(conn)

    print("Plotting results...")
    plot_bar([r[0] for r in top_players],[r[1] for r in top_players],"Top Players by Avg Rating","Player","Avg Rating")

    labels = []
    diffs = []
    for r in decisive:
        labels.append(f"M{r[0]} ({r[1]})")
        diffs.append(abs(int(r[2]) - int(r[3])))
    plot_bar(labels,diffs,"Decisive Matches (Score Difference >= 2)","Match","Score Difference")
    
    plot_bar([r[0] for r in team_win],[r[1] for r in team_win],"Team Win Rates","Team","Win Rate (%)")
    plot_pie([r[0] for r in country_counts],[r[1] for r in country_counts],"Player Ratio by Country")

    conn.close()
    print("Done.")
