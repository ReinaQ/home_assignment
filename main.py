import logging
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)


def get_response(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        logging.error(err)


def get_pokemon_types(pokemon):
    pokemon_types = pokemon["types"]
    return [t["type"]["name"] for t in pokemon_types]


def get_pokemon_games(pokemon):
    pokemon_games = pokemon["game_indices"]
    return [g["version"]["name"] for g in pokemon_games]


def get_pokemons_urls(pokemons_link):
    pokemons_urls = []
    while pokemons_link is not None:
        response = get_response(pokemons_link)
        results = response["results"]
        pokemons_urls.extend([r["url"] for r in results])
        pokemons_link = response["next"]
    return pokemons_urls


def get_pokemons_data(pokemons_urls):
    pokemons = []
    for url in pokemons_urls:
        response = get_response(url)
        pokemon = {"name": response["name"],
                   "id": response["id"],
                   "base_experience": response["base_experience"],
                   "weight_hg": response["weight"],
                   "height_dm": response["height"],
                   "order": response["order"],
                   "game_versions": get_pokemon_games(response),
                   "types": get_pokemon_types(response),
                   "front_default_sprite_url": response["sprites"]["front_default"]
                   }
        pokemons.append(pokemon)
    return pokemons


def is_in_specified_games(available_games):
    games_required = {"red", "blue", "leafgreen", "white"}
    common_elements = games_required.intersection(set(available_games))
    return len(common_elements) != 0


def main(pokemons_link):
    logging.info("Start to retrieve Pokemons' data")

    pokemons_urls = get_pokemons_urls(pokemons_link)
    logging.info(f"Get all urls of available Pokemons. Count: {len(pokemons_urls)}")

    pokemons = get_pokemons_data(pokemons_urls)
    logging.info("Data retrieving completed, start to transform data")

    pokemon_df = pd.DataFrame(data=pokemons)

    pokemon_df["is_in_specified_games"] = pokemon_df["game_versions"].apply(is_in_specified_games)
    pokemon_df_game_required = pokemon_df.query('is_in_specified_games == True')

    pokemon_df_game_required["name"] = pokemon_df_game_required["name"].str.title()

    pokemon_df_game_required["BMI"] = (0.1 * pokemon_df_game_required["weight_hg"]) \
                                      / ((0.1 * pokemon_df_game_required["height_dm"]) ** 2)

    pokemon_final = pokemon_df_game_required.drop(['is_in_specified_games'], axis=1)

    pokemon_final.to_csv("final_data.csv", index=False)

    logging.info("Data saved to current folder as a csv file")


if __name__ == "__main__":
    pokemons_base_link = "https://pokeapi.co/api/v2/pokemon?limit=100&offset=0"
    main(pokemons_base_link)