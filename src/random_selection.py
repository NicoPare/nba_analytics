# random match selection
from nba_api.live.nba.endpoints import scoreboard
import numpy as np

board = scoreboard.ScoreBoard()
games = board.games.get_dict()
game_id = [s["gameId"] for s in games]


games = [int(i) for i in game_id]
games = np.array(games)
games = games - 15
games = list(games)

games = [str(i) for i in games]


options = games
user_input = ""
input_message = "What match do you want to select?"

for index, item in enumerate(options):
    input_message += f"\n{index+1}) {item}\n"

input_message += "Your choice: "

while user_input not in options:
    user_input = input(input_message)

print("You picked: " + user_input)