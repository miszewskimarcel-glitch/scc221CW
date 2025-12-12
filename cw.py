import mysql.connector
import csv
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"

DB_NAME = "cs_tournament"


def create_mysql_connection(host, user, password):
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password
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


def populate_from_csv(conn, table, columns, csv_file):
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(columns))
    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cur.execute(query, row)

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


def q_avg_rating_by_map(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT m.map, AVG(perf.rating)
        FROM `Match` m
        JOIN Performance perf ON m.matchID = perf.matchID
        GROUP BY m.map
        ORDER BY AVG(perf.rating) DESC
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def q_team_winrates(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            t.name,
            ROUND(100 * SUM(CASE WHEN s.team_score > s.opp_score THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM (
            SELECT team1ID AS teamID, team1_score AS team_score, team2_score AS opp_score FROM `Match`
            UNION ALL
            SELECT team2ID AS teamID, team2_score AS team_score, team1_score AS opp_score FROM `Match`
        ) s
        JOIN Team t ON t.teamID = s.teamID
        GROUP BY t.teamID, t.name
        ORDER BY 2 DESC
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
    fig = go.Figure(go.Bar(
        x=x,
        y=y,
        text=[f"{v:.2f}" if isinstance(v, float) else v for v in y],
        textposition="outside"
    ))
    fig.update_layout(title=title, xaxis_title=xlab, yaxis_title=ylab)
    fig.show()


def plot_pie(labels, values, title):
    fig = go.Figure(go.Pie(labels=labels, values=values, textinfo="label+percent"))
    fig.update_layout(title=title)
    fig.show()


if __name__ == "__main__":
    conn = create_mysql_connection("", "", "")

    create_database(conn, DB_NAME)
    use_database(conn, DB_NAME)
    drop_tables(conn)
    create_tables(conn)

    populate_from_csv(conn, "Team",
        ["teamID","name","region","ranking"], "teams.csv")
    populate_from_csv(conn, "Player",
        ["playerID","teamID","username","country","role","details","overall_rating"], "players.csv")
    populate_from_csv(conn, "`Match`",
        ["matchID","map","date","duration","type_info","match_type","stage",
         "team1ID","team2ID","team1_score","team2_score"], "matches.csv")
    populate_from_csv(conn, "Performance",
        ["matchID","playerID","kills","deaths","assists","rating","details"], "performance.csv")

    top_players = q_top_players(conn)
    avg_map = q_avg_rating_by_map(conn)
    team_win = q_team_winrates(conn)
    country_counts = q_player_count_by_country(conn)

    plot_bar([r[0] for r in top_players], [r[1] for r in top_players],
             "Top Players by Avg Rating", "Player", "Avg Rating")

    plot_bar([r[0] for r in avg_map], [r[1] for r in avg_map],
             "Average Rating by Map", "Map", "Avg Rating")

    plot_bar([r[0] for r in team_win], [r[1] for r in team_win],
             "Team Win Rates", "Team", "Win Rate (%)")

    plot_pie([r[0] for r in country_counts], [r[1] for r in country_counts],
             "Player Ratio by Country")

    conn.close()
