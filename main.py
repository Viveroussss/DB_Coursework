import csv
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

NUM_PLAYERS = 100
NUM_STAFF = 20
NUM_LOCATIONS = 5
NUM_GAMES = 8
NUM_TABLES = 20
NUM_SLOT_MACHINES = 15
NUM_REWARDS = 10
NUM_PLAYER_GAMES = 300
NUM_STAFF_ASSIGNED = 50
NUM_SLOT_PLAYS = 200
NUM_PLAYER_REWARDS = 50
NUM_TRANSACTIONS = 300
NUM_GAME_RESULTS = 100
NUM_LOGIN_HISTORY = 200
NUM_AUDIT_LOGS = 50

# Helper to write CSV
def write_csv(filename, fieldnames, rows):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

# ======================== player ========================
def gen_players():
    players = []
    for _ in range(NUM_PLAYERS):
        dob = fake.date_of_birth(minimum_age=21, maximum_age=80)
        # Generate a phone number in a consistent format: +1-XXX-XXX-XXXX
        phone = f"+1-{fake.random_number(digits=3)}-{fake.random_number(digits=3)}-{fake.random_number(digits=4)}"
        players.append({
            'email': fake.unique.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'dob': dob.isoformat(),
            'phone': phone,
            'registration_date': fake.date_time_between(start_date='-2y', end_date='now').isoformat(sep=' '),
            'loyalty_points': random.randint(0, 10000),
        })
    return players

# ======================== staff ========================
def gen_staff():
    positions = ['Dealer', 'Manager', 'Security', 'Cashier', 'Technician']
    staff = []
    for _ in range(NUM_STAFF):
        hire_date = fake.date_between(start_date='-10y', end_date='-1y')
        staff.append({
            'staff_email': fake.unique.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'position': random.choice(positions),
            'hire_date': hire_date.isoformat(),
            'salary': round(random.uniform(30000, 100000), 2),
        })
    return staff

# ======================== casino_location ========================
def gen_locations():
    locations = []
    for i in range(NUM_LOCATIONS):
        loc_code = f"LOC{i+1:03}"
        locations.append({
            'location_code': loc_code,
            'name': f"{fake.city()} Casino",
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'country': fake.country(),
        })
    return locations

# ======================== game ========================
def gen_games():
    # Типы из последнего ограничения:
    game_types = ['Slot', 'Poker', 'Roulette', 'Blackjack', 'Number', 'Bingo']
    games = []
    for i in range(NUM_GAMES):
        gname = f"Game_{i+1}"
        gtype = random.choice(game_types)
        min_bet = round(random.uniform(1, 10), 2)
        max_bet = round(random.uniform(20, 1000), 2)
        if max_bet < min_bet:
            max_bet, min_bet = min_bet, max_bet
        games.append({
            'game_name': gname,
            'type': gtype,
            'min_bet': min_bet,
            'max_bet': max_bet,
        })
    return games

# ======================== player_game ========================
def gen_player_games(players, games):
    rows = []
    for _ in range(NUM_PLAYER_GAMES):
        p = random.choice(players)
        g = random.choice(games)
        registration_date = datetime.fromisoformat(p['registration_date'])
        play_time = fake.date_time_between(start_date=registration_date, end_date='now')
        amount_bet = round(random.uniform(g['min_bet'], g['max_bet']), 2)
        amount_won = round(random.uniform(0, amount_bet*3), 2)  # можно выиграть в 3 раза больше
        rows.append({
            'player_email': p['email'],
            'game_name': g['game_name'],
            'play_time': play_time.isoformat(sep=' '),
            'amount_bet': amount_bet,
            'amount_won': amount_won,
        })
    return rows

# ======================== table_game ========================
def gen_table_games(games, locations):
    statuses = ['Available', 'In Use', 'Maintenance']
    tables = []
    for i in range(NUM_TABLES):
        tcode = f"TBL{i+1:03}"
        g = random.choice(games)
        loc = random.choice(locations)
        tables.append({
            'table_code': tcode,
            'game_name': g['game_name'],
            'location_code': loc['location_code'],
            'status': random.choice(statuses),
        })
    return tables

# ======================== staff_assigned_tables ========================
def gen_staff_assigned(staff, tables):
    rows = []
    for _ in range(NUM_STAFF_ASSIGNED):
        s = random.choice(staff)
        t = random.choice(tables)
        shift_start = fake.date_time_between(start_date='-30d', end_date='-1d')
        shift_end = shift_start + timedelta(hours=8)
        rows.append({
            'staff_email': s['staff_email'],
            'table_code': t['table_code'],
            'shift_start': shift_start.isoformat(sep=' '),
            'shift_end': shift_end.isoformat(sep=' '),
        })
    return rows

# ======================== slot_machine ========================
def gen_slot_machines(locations):
    statuses = ['Online', 'Offline', 'Maintenance']
    machines = []
    for i in range(NUM_SLOT_MACHINES):
        mcode = f"SM{i+1:03}"
        loc = random.choice(locations)
        model = f"Model-{random.randint(100, 999)}"
        machines.append({
            'machine_code': mcode,
            'location_code': loc['location_code'],
            'status': random.choice(statuses),
            'model': model,
        })
    return machines

# ======================== slot_play ========================
def gen_slot_plays(machines, players):
    rows = []
    for _ in range(NUM_SLOT_PLAYS):
        m = random.choice(machines)
        p = random.choice(players)
        registration_date = datetime.fromisoformat(p['registration_date'])
        play_time = fake.date_time_between(start_date=registration_date, end_date='now')
        bet_amount = round(random.uniform(1, 100), 2)
        win_amount = round(random.uniform(0, bet_amount*5), 2)
        rows.append({
            'machine_code': m['machine_code'],
            'player_email': p['email'],
            'play_time': play_time.isoformat(sep=' '),
            'bet_amount': bet_amount,
            'win_amount': win_amount,
        })
    return rows

# ======================== reward ========================
def gen_rewards():
    rewards = []
    for i in range(NUM_REWARDS):
        code = f"RWD{i+1:03}"
        points = random.randint(100, 5000)
        rewards.append({
            'reward_code': code,
            'name': fake.word().capitalize() + " Reward",
            'points_required': points,
            'description': fake.sentence(nb_words=8),
        })
    return rewards

# ======================== player_reward ========================
def gen_player_rewards(players, rewards):
    rows = []
    for _ in range(NUM_PLAYER_REWARDS):
        p = random.choice(players)
        r = random.choice(rewards)
        registration_date = datetime.fromisoformat(p['registration_date'])
        redeem_date = fake.date_time_between(start_date=registration_date, end_date='now')
        rows.append({
            'player_email': p['email'],
            'reward_code': r['reward_code'],
            'redeem_date': redeem_date.isoformat(sep=' '),
        })
    return rows

# ======================== transaction ========================
def gen_transactions(players, games):
    types = ['Bet', 'Win', 'Deposit', 'Withdraw']
    rows = []
    for i in range(NUM_TRANSACTIONS):
        p = random.choice(players)
        g = random.choice(games + [None])  # game_name can be null sometimes?
        ttype = random.choice(types)
        amount = round(random.uniform(10, 1000), 2)
        registration_date = datetime.fromisoformat(p['registration_date'])
        ttime = fake.date_time_between(start_date=registration_date, end_date='now')
        rows.append({
            'transaction_code': f"TXN{i+1:06}",
            'player_email': p['email'],
            'amount': amount,
            'transaction_type': ttype,
            'transaction_time': ttime.isoformat(sep=' '),
            'game_name': g['game_name'] if g else '',
        })
    return rows

# ======================== game_result ========================
def gen_game_results(games, tables):
    rows = []
    for i in range(NUM_GAME_RESULTS):
        g = random.choice(games)
        t = random.choice(tables + [None])
        result_time = fake.date_time_between(start_date='-1y', end_date='now')
        outcome = fake.sentence(nb_words=10)
        rows.append({
            'result_code': f"RES{i+1:05}",
            'game_name': g['game_name'],
            'table_code': t['table_code'] if t else '',
            'result_time': result_time.isoformat(sep=' '),
            'outcome_description': outcome,
        })
    return rows

# ======================== login_history ========================
def gen_login_history(players):
    rows = []
    for i in range(NUM_LOGIN_HISTORY):
        p = random.choice(players)
        registration_date = datetime.fromisoformat(p['registration_date'])
        login_time = fake.date_time_between(start_date=registration_date, end_date='now')
        rows.append({
            'login_id': fake.uuid4(),
            'player_email': p['email'],
            'login_time': login_time.isoformat(sep=' '),
            'ip_address': fake.ipv4(),
            'device': fake.user_agent(),
        })
    return rows

# ======================== audit_log ========================
def gen_audit_logs(staff):
    event_types = ['Login', 'Logout', 'PasswordChange', 'TableAssignment', 'SystemUpdate']
    logs = []
    for i in range(NUM_AUDIT_LOGS):
        s = random.choice(staff + [None])
        event_time = fake.date_time_between(start_date='-1y', end_date='now')
        logs.append({
            'log_code': f"LOG{i+1:05}",
            'event_type': random.choice(event_types),
            'event_time': event_time.isoformat(sep=' '),
            'performed_by': s['staff_email'] if s else '',
            'details': fake.text(max_nb_chars=100),
        })
    return logs

# ======= main generation =======
def main():
    players = gen_players()
    staff = gen_staff()
    locations = gen_locations()
    games = gen_games()
    tables = gen_table_games(games, locations)
    staff_assigned = gen_staff_assigned(staff, tables)
    slot_machines = gen_slot_machines(locations)
    slot_plays = gen_slot_plays(slot_machines, players)
    rewards = gen_rewards()
    player_rewards = gen_player_rewards(players, rewards)
    player_games = gen_player_games(players, games)
    transactions = gen_transactions(players, games)
    game_results = gen_game_results(games, tables)
    login_history = gen_login_history(players)
    audit_logs = gen_audit_logs(staff)

    write_csv('generated/player.csv', players[0].keys(), players)
    write_csv('generated/staff.csv', staff[0].keys(), staff)
    write_csv('generated/casino_location.csv', locations[0].keys(), locations)
    write_csv('generated/game.csv', games[0].keys(), games)
    write_csv('generated/table_game.csv', tables[0].keys(), tables)
    write_csv('generated/staff_assigned_tables.csv', staff_assigned[0].keys(), staff_assigned)
    write_csv('generated/slot_machine.csv', slot_machines[0].keys(), slot_machines)
    write_csv('generated/slot_play.csv', slot_plays[0].keys(), slot_plays)
    write_csv('generated/reward.csv', rewards[0].keys(), rewards)
    write_csv('generated/player_reward.csv', player_rewards[0].keys(), player_rewards)
    write_csv('generated/player_game.csv', player_games[0].keys(), player_games)
    write_csv('generated/transaction.csv', transactions[0].keys(), transactions)
    write_csv('generated/game_result.csv', game_results[0].keys(), game_results)
    write_csv('generated/login_history.csv', login_history[0].keys(), login_history)
    write_csv('generated/audit_log.csv', audit_logs[0].keys(), audit_logs)

if __name__ == "__main__":
    main()
