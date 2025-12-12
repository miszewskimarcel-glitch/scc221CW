import csv
import mysql.connector
import plotly.graph_objects as go
import plotly.io as pio

DB_NAME = "cs_tournament"
pio.renderers.default = "browser"


def connect():
    return mysql.connector.connect(
        host="",
        user="",
        password=""
    )


def setup_database(cur):
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    cur.execute(f"CREATE DATABASE {DB_NAME}")
    cur.execute(f"USE {DB_NAME}")


def create_tables(cur):
    cur.execute("""
        CREATE TABLE Team (
            teamID INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            region VARCHAR(50),
            ranking INT
        )
    """)

    cur.execute("""
        CREATE TABLE Player (
            playerID INT PRIMARY KEY,
            teamID INT NOT NULL,
            username VARCHAR(50) NOT NULL,
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
            team1ID INT NOT NULL,
            team2ID INT NOT NULL,
            team1_score INT,
            team2_score INT,
            FOREIGN KEY (team1ID) REFERENCES Team(teamID),
            FOREIGN KEY (team2ID) REFERENCES Team(teamID)
        )
    """)

    cur.execute("""
        CREATE TABLE Performance (
            matchID INT NOT NULL,
            playerID INT NOT NULL,
            kills INT,
            deaths INT,
            assists INT,
            rating DECIMAL(4,2),
            details TEXT,
            PRIMARY KEY (matchID, playerID),
            FOREIGN KEY (matchID) REFERENCES `Match`(matchID),
            FOREIGN KEY (playerID) REFERENCES Player(playerID)
        )
    """)


def load_teams(cur, filename="teams.csv"):
    rows = []
    with open(filename, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((
                int(r["teamID"]),
                r["name"],
                r.get("region") or None,
                int(r["ranking"]) if r.get("ranking") not in (None, "") else None
            ))
    cur.executemany(
        "INSERT INTO Team (teamID, name, region, ranking) VALUES (%s, %s, %s, %s)",
        rows
    )


def load_players(cur, filename="players.csv"):
    rows = []
    with open(filename, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            overall = r.get("overall_rating", "")
            rows.append((
                int(r["playerID"]),
                int(r["teamID"]),
                r["username"],
                r.get("country") or None,
                r.get("role") or None,
                r.get("details", "") or "",
                float(overall) if overall != "" else None
            ))
    cur.executemany(
        """INSERT INTO Player
           (playerID, teamID, username, country, role, details, overall_rating)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        rows
    )


def load_matches(cur, filename="matches.csv"):
    rows = []
    with open(filename, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((
                int(r["matchID"]),
                r.get("map") or None,
                r["date"],
                int(r["duration"]) if r.get("duration") not in (None, "") else None,
                r.get("type_info") or None,
                r.get("match_type") or None,
                r.get("stage") or None,
                int(r["team1ID"]),
                int(r["team2ID"]),
                int(r["team1_score"]) if r.get("team1_score") not in (None, "") else None,
                int(r["team2_score"]) if r.get("team2_score") not in (None, "") else None
            ))
    cur.executemany(
        """INSERT INTO `Match`
           (matchID, map, date, duration, type_info, match_type, stage,
            team1ID, team2ID, team1_score, team2_score)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        rows
    )


def load_performance(cur, filename="performance.csv"):
    rows = []
    with open(filename, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((
                int(r["matchID"]),
                int(r["playerID"]),
                int(r["kills"]),
                int(r["deaths"]),
                int(r["assists"]),
                float(r["rating"]),
                r.get("details", "") or ""
            ))
    cur.executemany(
        """INSERT INTO Performance
           (matchID, playerID, kills, deaths, assists, rating, details)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        rows
    )


def q_top_players(cur, limit=10):
    cur.execute("""
        SELECT
            p.username,
            AVG(perf.rating) AS avg_rating
        FROM Player p
        JOIN Performance perf ON p.playerID = perf.playerID
        GROUP BY p.playerID, p.username
        ORDER BY avg_rating DESC
        LIMIT %s
    """, (limit,))
    return cur.fetchall()


def q_avg_rating_by_map(cur):
    cur.execute("""
        SELECT
            m.map,
            AVG(perf.rating) AS avg_rating
        FROM `Match` m
        JOIN Performance perf ON m.matchID = perf.matchID
        GROUP BY m.map
        ORDER BY avg_rating DESC
    """)
    return cur.fetchall()


def q_team_winrates(cur):
    cur.execute("""
        SELECT
            t.name,
            COUNT(*) AS matches_played,
            SUM(CASE WHEN s.team_score > s.opp_score THEN 1 ELSE 0 END) AS wins,
            ROUND(100 * SUM(CASE WHEN s.team_score > s.opp_score THEN 1 ELSE 0 END) / COUNT(*), 2) AS win_rate
        FROM (
            SELECT matchID, team1ID AS teamID, team2ID AS oppID, team1_score AS team_score, team2_score AS opp_score
            FROM `Match`
            UNION ALL
            SELECT matchID, team2ID AS teamID, team1ID AS oppID, team2_score AS team_score, team1_score AS opp_score
            FROM `Match`
        ) s
        JOIN Team t ON t.teamID = s.teamID
        GROUP BY t.teamID, t.name
        ORDER BY win_rate DESC, wins DESC;
    """)
    return cur.fetchall()


def q_best_player_per_country(cur):
    cur.execute("""
        WITH per_player AS (
            SELECT
                p.country,
                p.username,
                AVG(perf.rating) AS avg_rating,
                SUM(perf.kills)  AS total_kills
            FROM Player p
            JOIN Performance perf ON p.playerID = perf.playerID
            WHERE p.country IS NOT NULL AND p.country <> ''
            GROUP BY p.playerID, p.country, p.username
        ),
        ranked AS (
            SELECT
                country,
                username,
                avg_rating,
                total_kills,
                ROW_NUMBER() OVER (
                    PARTITION BY country
                    ORDER BY avg_rating DESC, total_kills DESC, username ASC
                ) AS rn
            FROM per_player
        )
        SELECT country, username, avg_rating, total_kills
        FROM ranked
        WHERE rn = 1
        ORDER BY avg_rating DESC, country ASC;
    """)
    return cur.fetchall()


def q_player_count_by_country(cur):
    cur.execute("""
        SELECT
            country,
            COUNT(*) AS player_count
        FROM Player
        WHERE country IS NOT NULL AND country <> ''
        GROUP BY country
        ORDER BY player_count DESC, country ASC;
    """)
    return cur.fetchall()

def plot_top_players(rows):
    players = [r[0] for r in rows]
    avg = [float(r[1]) for r in rows]

    fig = go.Figure(go.Bar(
        x=players,
        y=avg,
        text=[f"{v:.2f}" for v in avg],
        textposition="outside"
    ))
    fig.update_layout(
        title="Top Players by Average Rating",
        xaxis_title="Player",
        yaxis_title="Avg Rating"
    )
    fig.show()


def plot_rating_by_map(rows):
    maps = [r[0] for r in rows]
    avg = [float(r[1]) for r in rows]

    fig = go.Figure(go.Bar(
        x=maps,
        y=avg,
        text=[f"{v:.2f}" for v in avg],
        textposition="outside"
    ))
    fig.update_layout(
        title="Average Rating by Map",
        xaxis_title="Map",
        yaxis_title="Avg Rating"
    )
    fig.show()


def plot_team_winrates(rows):
    teams = [r[0] for r in rows]
    win_rates = [float(r[3]) for r in rows]

    fig = go.Figure(go.Bar(
        x=teams,
        y=win_rates,
        text=[f"{v:.2f}" for v in win_rates],
        textposition="outside"
    ))
    fig.update_layout(
        title="Team Win Rates",
        xaxis_title="Team",
        yaxis_title="Win Rate (%)"
    )
    fig.show()


def plot_best_country_players(rows):
    countries = [r[0] for r in rows]
    players = [r[1] for r in rows]
    avg = [float(r[2]) for r in rows]
    kills = [int(r[3]) for r in rows]

    hover = [
        f"Player: {p}<br>Avg Rating: {a:.2f}<br>Total Kills: {k}"
        for p, a, k in zip(players, avg, kills)
    ]

    fig = go.Figure(go.Bar(
        x=countries,
        y=avg,
        text=[f"{v:.2f}" for v in avg],
        textposition="outside",
        hovertext=hover,
        hoverinfo="text"
    ))
    fig.update_layout(
        title="Best Player per Country (by Avg Rating)",
        xaxis_title="Country",
        yaxis_title="Avg Rating"
    )
    fig.show()


def plot_country_player_ratio(rows, top_n=10):
    rows = [(c, int(n)) for c, n in rows]
    top = rows[:top_n]
    other = sum(n for _, n in rows[top_n:])

    labels = [c for c, _ in top]
    values = [n for _, n in top]

    if other > 0:
        labels.append("Other")
        values.append(other)

    fig = go.Figure(go.Pie(
        labels=labels,
        values=values,
        textinfo="label+percent"
    ))
    fig.update_layout(title="Player Ratio by Country")
    fig.show()


def main():
    conn = connect()
    cur = conn.cursor()

    setup_database(cur)
    create_tables(cur)

    load_teams(cur)
    load_players(cur)
    load_matches(cur)
    load_performance(cur)
    conn.commit()

    top_players = q_top_players(cur, limit=10)
    avg_map = q_avg_rating_by_map(cur)
    team_win = q_team_winrates(cur)
    best_country = q_best_player_per_country(cur)
    country_counts = q_player_count_by_country(cur)

    plot_top_players(top_players)
    plot_rating_by_map(avg_map)
    plot_team_winrates(team_win)
    plot_best_country_players(best_country)
    plot_country_player_ratio(country_counts, top_n=10)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
