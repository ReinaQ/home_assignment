import asyncio
import aiohttp
import logging
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO)


def get_response(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as err:
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


async def get_pokemon(client, pokemon_url):
    try:
        async with client.get(pokemon_url) as response:
            pokemon_raw = await response.json()
            pokemon = {"name": pokemon_raw["name"],
                       "id": pokemon_raw["id"],
                       "base_experience": pokemon_raw["base_experience"],
                       "weight_hg": pokemon_raw["weight"],
                       "height_dm": pokemon_raw["height"],
                       "order": pokemon_raw["order"],
                       "game_versions": get_pokemon_games(pokemon_raw),
                       "types": get_pokemon_types(pokemon_raw),
                       "front_default_sprite_url": pokemon_raw["sprites"]["front_default"]
                       }
            return pokemon
    except Exception as err:
        logging.error(err)


async def get_pokemons_data(pokemons_urls):
    async with aiohttp.ClientSession(raise_for_status=True) as client:
        tasks = []
        for url in pokemons_urls:
            tasks.append(asyncio.create_task(get_pokemon(client, url)))

        pokemons = await asyncio.gather(*tasks)
    return pokemons


def is_in_specified_games(available_games):
    games_required = {"red", "blue", "leafgreen", "white"}
    common_elements = games_required.intersection(set(available_games))
    return len(common_elements) != 0


def process_pokemons(pokemons):
    pokemon_df = pd.DataFrame(data=pokemons)

    pokemon_df["is_in_specified_games"] = pokemon_df["game_versions"].apply(is_in_specified_games)
    pokemon_df_game_required = pokemon_df.query('is_in_specified_games == True')

    pokemon_df_game_required["name"] = pokemon_df_game_required["name"].str.title()

    pokemon_df_game_required["BMI"] = (0.1 * pokemon_df_game_required["weight_hg"]) \
                                      / ((0.1 * pokemon_df_game_required["height_dm"]) ** 2)

    pokemon_final = pokemon_df_game_required.drop(['is_in_specified_games'], axis=1)
    return pokemon_final


async def main(pokemons_link):
    logging.info("Start to retrieve Pokemons' data")

    pokemons_urls = get_pokemons_urls(pokemons_link)
    logging.info(f"Get all urls of available Pokemons. Count: {len(pokemons_urls)}")

    pokemons = await get_pokemons_data(pokemons_urls)
    logging.info("Data retrieving completed, start to transform data")

    pokemon_final = process_pokemons(pokemons)
    pokemon_final.to_csv("final_data.csv", index=False)
    logging.info("Data saved to current folder as a csv file")


if __name__ == "__main__":
    pokemons_base_link = "https://pokeapi.co/api/v2/pokemon?limit=100&offset=0"
    asyncio.run(main(pokemons_base_link))
